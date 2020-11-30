package com.ovoenergy.comms.deduplication

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import cats.effect._
import cats.implicits._

import munit._
import org.scalacheck.Arbitrary

import software.amazon.awssdk.services.dynamodb.{model => _, _}
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse

import com.ovoenergy.comms.deduplication.TestUtils._
import com.ovoenergy.comms.deduplication.dynamodb.DynamoDbConfig
import com.ovoenergy.comms.deduplication.dynamodb.DynamoDbProcessRepo
import com.ovoenergy.comms.deduplication.utils._

class DynamoDbProcessRepoSuite extends FunSuite {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  def given[A: Arbitrary]: IO[A] =
    IO.fromOption(Arbitrary.arbitrary[A].sample)(
      new RuntimeException("Unable to generate a sample")
    )

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

  case class Resources(
      config: DynamoDbConfig,
      dynamoclient: DynamoDbAsyncClient,
      processRepoR: ProcessRepo[IO, UUID, UUID]
  ) {

    def getItem(id: UUID, processorId: UUID): IO[Option[AttributeValue]] = {
      val request = GetItemRequest
        .builder()
        .tableName(config.tableName.value)
        .key(
          Map(
            "id" -> AttributeValue.builder.s(id.toString()).build(),
            "processorId" -> AttributeValue.builder.s(processorId.toString()).build()
          ).asJava
        )
        .consistentRead(true)
        .build()

      val response = fromCompletableFuture[IO, GetItemResponse] { () =>
        dynamoclient
          .getItem(request)
      }

      response.map { r =>
        if (r.hasItem()) {
          AttributeValue.builder().m(r.item).build.some
        } else {
          none[AttributeValue]
        }
      }
    }

  }

  val resources: Resource[IO, Resources] = for {
    dynamoClient <- dynamoClientResource[IO]
    tableName <- Resource.liftF(randomTableName[IO])
    table <- tableResource[IO](dynamoClient, tableName)
    config = DynamoDbConfig(DynamoDbConfig.TableName(table))
  } yield Resources(
    config,
    dynamoClient,
    DynamoDbProcessRepo[IO, UUID, UUID](
      config,
      dynamoClient
    )
  )
}