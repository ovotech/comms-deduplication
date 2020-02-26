package com.ovoenergy.comms.deduplication

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.collection.JavaConverters._

import cats.effect._
import cats.implicits._

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.AttributeValue

import model._


class DeduplicationSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  "Deduplication" should "always process event for the first time" in forAll { (processorId: UUID, id1: UUID, id2: UUID) =>

    val result = deduplicationResource(processorId)
      .use { ps =>
        for {
          a <- ps.protect(id1, IO("nonProcessed"), IO("processed"))
          b <- ps.protect(id2, IO("nonProcessed"), IO("processed"))
        } yield (a, b)
      }
      .unsafeRunSync()

    val (a, b) = result
    a shouldBe "nonProcessed"
    b shouldBe "nonProcessed"
  }

  it should "process event for the first time, ignore after that" in forAll { (processorId: UUID, id: UUID) =>

    val result = deduplicationResource(processorId)
      .use { ps =>
        for {
          a <- ps.protect(id, IO("nonProcessed"), IO("processed"))
          b <- ps.protect(id, IO("nonProcessed"), IO("processed"))
        } yield (a, b)
      }
      .unsafeRunSync()

    val (a, b) = result
    a shouldBe "nonProcessed"
    b shouldBe "processed"
  }

  it should "process event for the first time, ignore other before expiration" in forAll { (processorId: UUID, id: UUID) =>

    val result = deduplicationResource(processorId, 5.seconds)
      .use { ps =>
        for {
          a <- ps.processing(id).map {
            case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
            case _ => "processed"
          }
          b <- ps.processing(id).map {
            case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
            case _ => "processed"
          }
        } yield (a, b)
      }
      .unsafeRunSync()

    val (a, b) = result
    a shouldBe "nonProcessed"
    b shouldBe "processed"
  }

  it should "process event for the first time, accept other after expiration" in forAll { (processorId: UUID, id: UUID) =>

    val result = deduplicationResource(processorId, 1.seconds)
      .use { ps =>
        for {
          a <- ps.processing(id).map {
            case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
            case _ => "processed"
          }
          _ <- IO.sleep(5.seconds)
          b <- ps.processing(id).map {
            case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
            case _ => "processed"
          }
        } yield (a, b)
      }
      .unsafeRunSync()

    val (a, b) = result
    a shouldBe "nonProcessed"
    b shouldBe "nonProcessed"
  }

  it should "set startedAt > completedAt when try to process a duplicate" in forAll { (processorId: UUID, id: UUID) =>

    val rs = for {
      tableName <- tableResource
      dynamoDb <- dynamoDbR
      deduplication <- deduplicationResource(tableName, processorId, 5.seconds)
    } yield (tableName, dynamoDb, deduplication)

    rs.use {
        case (tableName, dynamoDb, deduplication) =>
          deduplication.protect(
            id,
            IO(note("Processed correctly")),
            IO.raiseError(new Exception("Impossible to skip the first time"))
          ) >>
            deduplication.protect(
              id,
              IO.raiseError(new Exception("Impossible to process the second time")),
              IO(note("Skipped correctly"))
            ) >>
            IO(
              dynamoDb.getItem(
                new GetItemRequest()
                  .withConsistentRead(true)
                  .withTableName(tableName)
                  .withKey(
                    Map(
                      "id" -> new AttributeValue().withS(id.toString),
                      "processorId" -> new AttributeValue().withS(processorId.toString())
                    ).asJava
                  )
              )
            ).map { res =>
              val startedAt = res.getItem().get("startedAt").getN().toLong
              val completedAt = res.getItem().get("completedAt").getN().toLong

              startedAt should be > completedAt
            }
      }
      .unsafeRunSync()
  }

  it should "process only one event out of multiple concurrent events" in forAll(MinSuccessful(1)) { (processorId: UUID, id: UUID) =>

    val n = 120

    val result = deduplicationResource(processorId)
      .use { ps =>
        List.fill(math.abs(n))(id).parTraverse { i =>
          ps.protect(i, IO(1), IO(0))
        }
      }
      .unsafeRunSync()

    result.sum shouldBe 1
  }

  val tableResource = Resource.liftF(IO(sys.env.getOrElse("TEST_TABLE", "phil-processing")))

  val dynamoDbR: Resource[IO, AmazonDynamoDBAsync] =
    Resource.make(Sync[IO].delay(AmazonDynamoDBAsyncClientBuilder.defaultClient()))(c =>
      Sync[IO].delay(c.shutdown())
    )

  def deduplicationResource(
      processorId: UUID,
      maxProcessingTime: FiniteDuration = 5.seconds
  ): Resource[IO, Deduplication[IO, UUID]] =
    for {
      tableName <- tableResource
      deduplication <- deduplicationResource(tableName, processorId, maxProcessingTime)
    } yield deduplication

  def deduplicationResource(
      tableName: String,
      processorId: UUID,
      maxProcessingTime: FiniteDuration
  ): Resource[IO, Deduplication[IO, UUID]] =
    for {
      deduplication <- Deduplication.resource[IO, UUID, UUID](
        Config(
          tableName = Config.TableName(tableName),
          processorId = processorId,
          maxProcessingTime = maxProcessingTime,
          ttl = 1.day,
          pollStrategy = PollStrategy.backoff(maxDuration = 30.seconds)
        )
      )
    } yield deduplication
}
