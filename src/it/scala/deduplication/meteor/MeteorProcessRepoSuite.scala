package com.ovoenergy.comms.deduplication.meteor

import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.{meteor => _, _}
import java.time.Instant
import java.time.temporal.TemporalUnit
import java.util.UUID
import java.util.concurrent.TimeUnit
import meteor._
import munit._
import org.scalacheck.Arbitrary
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import software.amazon.awssdk.services.dynamodb.model.BillingMode

class MeteorProcessRepoSuite extends FunSuite {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  private def createTestTable(client: Client[IO], name: String) = {
    val partitionKey = KeyDef[String]("id", DynamoDbType.S)
    val sortKey = KeyDef[String]("contextId", DynamoDbType.S)
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

  test("should segregate processes by context") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        contextId1 <- uuidF.map(_.toString())
        contextId2 <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, contextId1, now)
        _ <- repo.create(id, contextId2, later)
        _ <- repo.markAsCompleted(id, contextId2, "testresult", later, none)
        process1 <- repo.get(id, contextId1)
        process2 <- repo.get(id, contextId2)
      } yield {
        assertEquals(clue(process1).isDefined, true)
        assertEquals(clue(process2).isDefined, true)
        assert(clue(process1).exists(_.startedAt.equals(clue(now))))
        assert(clue(process2).exists(_.startedAt.equals(clue(later))))
        assert(clue(process2).exists(_.result.contains("testresult")))
      }
    }
  }

  test("`create` should add the record in dynamo") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        _ <- repo.create(id, contextId, now)
        process <- repo.get(id, contextId)
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
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, contextId, now)
        process <- repo.create(id, contextId, later)
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
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, contextId, now)
        _ <- repo.create(id, contextId, later)
        process <- repo.get(id, contextId)
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
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(id, contextId, "testresult", now, 10.seconds.some)
        process <- repo.get(id, contextId)
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
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(id, contextId, "testresult", now, none)
        process <- repo.get(id, contextId)
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
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(id, contextId, "testresult", now, none)
        _ <- repo.attemptReplacing(id, contextId, now, later)
        process <- repo.get(id, contextId)
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
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli _)
        later = Instant.ofEpochMilli(now.toEpochMilli() + 1000)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(id, contextId, "testresult", now, none)
        _ <- repo.attemptReplacing(id, contextId, later, later)
        process <- repo.get(id, contextId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(now))))
        assert(clue(process).exists(_.result.contains("testresult")))
      }
    }
  }

}
