package com.ovoenergy.comms.deduplication

import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.collection.JavaConverters._

import cats.effect._
import cats.effect.concurrent._
import cats.implicits._

import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClientBuilder

import model._
import Config._

class DeduplicationSpec extends AnyFlatSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  behavior of "Deduplication"

  it should "always process event for the first time" in forAll {
    (processorId: UUID, id1: UUID, id2: UUID) =>

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

  it should "process event for the first time, ignore after that" in forAll {
    (processorId: UUID, id: UUID) =>

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

  it should "re-process the event if it failes the first time" in forAll {
    (processorId: UUID, id: UUID) =>

      val result = deduplicationResource(processorId, 1.seconds)
        .use { ps =>
          for {
            ref <- Ref[IO].of(0)
            _ <- ps.protect(id, IO.raiseError[Unit](new Exception("Expected exception")), IO.unit).attempt
            _ <- ps.protect(id, ref.set(1), IO.unit)
            _ <- ps.protect(id, ref.set(3), IO.unit)
            result <- ref.get
          } yield result
        }
        .unsafeRunSync()

      result shouldBe 1
  }

  it should "process event for the first time, accept other after expiration" in forAll {
    (processorId: UUID, id: UUID) =>

      val maxProcessingTime = 1.seconds
      val result = deduplicationResource(processorId, maxProcessingTime)
        .use { ps =>
          for {
            a <- ps.tryStartProcess(id)
            _ <- IO.sleep(maxProcessingTime + 1.second)
            bAndTime <- time(ps.tryStartProcess(id))
          } yield (a, bAndTime)
        }
        .unsafeRunSync()

      val (x, (y, yTime)) = result
      x shouldBe a[Outcome.New[IO]]
      y shouldBe a[Outcome.New[IO]]
      yTime should be < maxProcessingTime
  }

  it should "process only one event out of multiple concurrent events" in forAll(MinSuccessful(1)) {
    (processorId: UUID, id: UUID) =>

      val n = 120

      val result = deduplicationResource(processorId, maxProcessingTime = 30.seconds)
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

  def time[F[_]: Sync: Clock, A](fa: F[A]): F[(A, FiniteDuration)] =
    for {
      start <- Clock[F].monotonic(TimeUnit.MILLISECONDS)
      a <- fa
      end <- Clock[F].monotonic(TimeUnit.MILLISECONDS)
    } yield (a, (end - start).millis)
}
