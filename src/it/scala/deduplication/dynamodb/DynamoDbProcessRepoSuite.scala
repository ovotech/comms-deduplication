package com.ovoenergy.comms.deduplication

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.collection.JavaConverters._

import cats.effect._
import cats.effect.concurrent._
import cats.implicits._

import munit._

import software.amazon.awssdk.services.dynamodb.{model => _, _}

import model._
import Config._
import dynamodb.DynamoDbProcessRepo
import dynamodb.DynamoDbConfig
import org.scalacheck.Arbitrary
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse

class DynamoDbProcessRepoSuite extends FunSuite {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  def given[A: Arbitrary]: IO[A] = IO.fromOption(Arbitrary.arbitrary[A].sample)(new RuntimeException("Unable to generate a sample"))

  test("startProcessingUpdate should add the record in dynamo") {

    resources.use { resources =>

      for {
        id <- given[UUID]
        processorId <- given[UUID]
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
        _ <- resources.processRepoR.startProcessingUpdate(
          id = id, 
          processorId = processorId,
          now = now
        )
        optItem <- resources.getItem(id, processorId)
      } yield assertEquals(optItem.isDefined, true, clue(optItem))
    }
  }

  test("startProcessingUpdate should populate only startedAt when there is not previous record") {

    resources.use { resources =>

      for {
        id <- given[UUID]
        processorId <- given[UUID]
        now <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
        _ <- resources.processRepoR.startProcessingUpdate(
          id = id, 
          processorId = processorId,
          now = now
        )
        optItem <- resources.getItem(id, processorId)
        item <- IO.fromOption(optItem)(new RuntimeException("Item not found"))
      } yield {

        val testeeStartedAt = Option(item.m)
          .flatMap { m =>
            Option(m.get(DynamoDbProcessRepo.field.startedAt))
          }
          .flatMap { id =>
            Option(id.n())
          }
          .map { n =>
            Instant.ofEpochMilli(n.toLong)
          }

        assertEquals(clue(testeeStartedAt), now.some, clue(item))

        val testeeCompletedAt = Option(item.m)
          .flatMap { m =>
            Option(m.get(DynamoDbProcessRepo.field.completedAt))
          }

        assertEquals(clue(testeeCompletedAt), None, clue(item))

        val testeeExpiredOn = Option(item.m)
          .flatMap { m =>
            Option(m.get(DynamoDbProcessRepo.field.expiresOn))
          }

        assertEquals(clue(testeeExpiredOn), None, clue(item))
      }
    }
  }

  // if completed -> no
  // if not completed -> yes
  test("startProcessingUpdate should update startedAt when there is a previous non-completed record") {

    resources.use { resources =>

      for {
        id <- given[UUID]
        processorId <- given[UUID]
        nowOne <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
        _ <- resources.processRepoR.startProcessingUpdate(
          id = id, 
          processorId = processorId,
          now = nowOne
        )
        nowTwo = nowOne.plusSeconds(5)
        _ <- resources.processRepoR.startProcessingUpdate(
          id = id, 
          processorId = processorId,
          now = nowTwo
        )
        optItem <- resources.getItem(id, processorId)
        item <- IO.fromOption(optItem)(new RuntimeException("Item not found"))
      } yield {

        val testeeStartedAt = Option(item.m)
          .flatMap { m =>
            Option(m.get(DynamoDbProcessRepo.field.startedAt))
          }
          .flatMap { id =>
            Option(id.n())
          }
          .map { n =>
            Instant.ofEpochMilli(n.toLong)
          }

        assertEquals(clue(testeeStartedAt), nowTwo.some, clue(item))

        val testeeCompletedAt = Option(item.m)
          .flatMap { m =>
            Option(m.get(DynamoDbProcessRepo.field.completedAt))
          }

        assertEquals(clue(testeeCompletedAt), None, clue(item))

        val testeeExpiredOn = Option(item.m)
          .flatMap { m =>
            Option(m.get(DynamoDbProcessRepo.field.expiresOn))
          }

        assertEquals(clue(testeeExpiredOn), None, clue(item))
      }
    }
  }

  test("startProcessingUpdate should update startedAt when there is a previous completed record") {

    resources.use { resources =>

      for {
        id <- given[UUID]
        processorId <- given[UUID]
        nowOne <- Clock[IO].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
        _ <- resources.processRepoR.startProcessingUpdate(
          id = id, 
          processorId = processorId,
          now = nowOne
        )
        _ <- resources.processRepoR.completeProcess(
          id = id, 
          processorId = processorId,
          now = nowOne.plusMillis(100),
          ttl = 5.seconds
        )
        nowTwo = nowOne.plusSeconds(5)
        _ <- resources.processRepoR.startProcessingUpdate(
          id = id, 
          processorId = processorId,
          now = nowTwo
        )
        optItem <- resources.getItem(id, processorId)
        item <- IO.fromOption(optItem)(new RuntimeException("Item not found"))
      } yield {

        val testeeStartedAt = Option(item.m)
          .flatMap { m =>
            Option(m.get(DynamoDbProcessRepo.field.startedAt))
          }
          .flatMap { id =>
            Option(id.n())
          }
          .map { n =>
            Instant.ofEpochMilli(n.toLong)
          }

        assertEquals(clue(testeeStartedAt), nowTwo.some, clue(item))
      }
    }
  }

  case class Resources(
    config: DynamoDbConfig,
    dynamoclient: DynamoDbAsyncClient,
    processRepoR: ProcessRepo[IO, UUID, UUID]
  ) {

    def getItem(id: UUID, processorId: UUID): IO[Option[AttributeValue]] = {
      val request = GetItemRequest.builder()
        .tableName(config.tableName.value)
        .key(
          Map(
            "id" -> AttributeValue.builder.s(id.toString()).build(),
            "processorId" -> AttributeValue.builder.s(processorId.toString()).build()
          ).asJava
        )
        .consistentRead(true)
        .build()
      

      val response = DynamoDbProcessRepo.fromCompletableFuture[IO, GetItemResponse]{() => dynamoclient
        .getItem(request)
      }

      response.map { r => 
        if(r.hasItem()) {
          AttributeValue.builder().m(r.item).build.some
        } else {
          none[AttributeValue]
        }
      }
    }

  }

  // TODO Create and destroy the table
  val tableResource = Resource.liftF(IO(sys.env.getOrElse("TEST_TABLE", "phil-processing")))

  val dynamoDbR: Resource[IO, DynamoDbAsyncClient] =
    Resource.make(Sync[IO].delay(DynamoDbAsyncClient.builder.build()))(c =>
      Sync[IO].delay(c.close())
    )

  val resources: Resource[IO, Resources] = for {
    table <- tableResource
    dynamoclient <- dynamoDbR
    config = DynamoDbConfig(DynamoDbConfig.TableName(table))
  } yield Resources(
    config,
    dynamoclient,
    DynamoDbProcessRepo[IO, UUID, UUID](
      config,
      dynamoclient
    )
  )
}
