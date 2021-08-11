package com.ovoenergy.comms.deduplication
package meteorrepo

import munit._
import cats.effect._
import cats.implicits._
import meteor._
import java.util.UUID
import org.scalacheck.Arbitrary
import java.util.concurrent.TimeUnit
import java.time.Instant
import scala.concurrent.ExecutionContext
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import java.time.temporal.TemporalUnit
import scala.concurrent.duration._

class MeteorProcessRepoSuite extends FunSuite {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  private def createTestTable(client: Client[IO], name: String) = {
    val partitionKey = KeyDef[String]("id", DynamoDbType.S)
    val sortKey = KeyDef[String]("processorId", DynamoDbType.S)
    client
      .createCompositeKeysTable(
        name,
        partitionKey,
        sortKey,
        BillingMode.PAY_PER_REQUEST
      )
      .map(_ => CompositeKeysTable(name, partitionKey, sortKey))
  }

  private def deleteTable(client: Client[IO], name: String) =
    client.deleteTable(name)

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  val uuidF = IO(UUID.randomUUID())

  val testRepo: Resource[IO, ProcessRepo[IO, String, String, String]] =
    for {
      uuid <- Resource.eval(uuidF)
      tableName = s"comms-deduplication-test-${uuid}"
      client <- Client.resource[IO]
      table <- Resource.make[IO, CompositeKeysTable[String, String]](
        IO(println(s"Creating table ${tableName}")) >> createTestTable(client, tableName)
      )(_ => deleteTable(client, tableName))
    } yield MeteorProcessRepo[IO, String, String, String](client, table, readConsistently = true)

  test("`create` should add the record in dynamo") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        processorId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        _ <- repo.create(id, processorId, now)
        process <- repo.get(id, processorId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(now))))
      }
    }
  }

  test("`create` should return existing process") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        processorId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, processorId, now)
        process <- repo.create(id, processorId, later)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(now))))
      }
    }
  }

  test("`create` should not change an existing process") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        processorId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, processorId, now)
        _ <- repo.create(id, processorId, later)
        process <- repo.get(id, processorId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(now))))
      }
    }
  }

  test("`markAsCompleted` should store the result of F[A]") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        processorId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, processorId, now)
        _ <- repo.markAsCompleted(id, processorId, "testresult", now, 10.seconds.some)
        process <- repo.get(id, processorId)
      } yield {
        val expectedExpiration = Instant.ofEpochMilli(now.toEpochMilli() + 10000)
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.result.contains("testresult")))
        assert(clue(process).exists(_.expiresOn.contains(clue(expectedExpiration))))
      }
    }
  }

  test("`markAsCompleted` should be able to store a result with no expiration") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        processorId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, processorId, now)
        _ <- repo.markAsCompleted(id, processorId, "testresult", now, none)
        process <- repo.get(id, processorId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.result.contains("testresult")))
        assert(clue(process).exists(_.expiresOn.isEmpty))
      }
    }
  }

  test("`attemptReplacing` should replace the `startedAt` field and remove `result`") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        processorId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, processorId, now)
        _ <- repo.markAsCompleted(id, processorId, "testresult", now, none)
        _ <- repo.attemptReplacing(id, processorId, now, later)
        process <- repo.get(id, processorId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(later))))
        assert(clue(process).exists(_.result.isEmpty))
      }
    }
  }

  test("`attemptReplacing` should only update the process if the `startedAt` field did not change") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        processorId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, processorId, now)
        _ <- repo.markAsCompleted(id, processorId, "testresult", now, none)
        _ <- repo.attemptReplacing(id, processorId, later, later)
        process <- repo.get(id, processorId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(now))))
        assert(clue(process).exists(_.result.contains("testresult")))
      }
    }
  }

}
