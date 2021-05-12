package com.ovoenergy.comms.deduplication

import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import cats.effect._
import cats.effect.concurrent._
import cats.implicits._

import munit._

import model._
import Config._
import org.scalacheck.Arbitrary

import com.ovoenergy.comms.deduplication.TestUtils._

class DeduplicationSuite extends FunSuite {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  def given[A: Arbitrary]: A = Arbitrary.arbitrary[A].sample.get

  test("Deduplication should always process event for the first time") {
    val processorId = given[UUID]
    val id1 = given[UUID]
    val id2 = given[UUID]

    deduplicationResource(processorId)
      .use { ps =>
        for {
          a <- ps.protect(id1, IO("nonProcessed"), IO("processed"))
          b <- ps.protect(id2, IO("nonProcessed"), IO("processed"))
        } yield {
          assertEquals(clue(a), "nonProcessed")
          assertEquals(clue(b), "nonProcessed")
        }
      }
  }

  test("Deduplication should process event for the first time, ignore after that") {

    val processorId = given[UUID]
    val id = given[UUID]

    deduplicationResource(processorId)
      .use { ps =>
        for {
          a <- ps.protect(id, IO("nonProcessed"), IO("processed"))
          b <- ps.protect(id, IO("nonProcessed"), IO("processed"))
        } yield {
          assertEquals(clue(a), "nonProcessed")
          assertEquals(clue(b), "processed")
        }
      }
  }

  test("Deduplication should re-process the event if it failed the first time") {

    val processorId = given[UUID]
    val id = given[UUID]

    deduplicationResource(processorId, 1.seconds)
      .use { ps =>
        for {
          ref <- Ref[IO].of(0)
          _ <- ps
            .protect(id, IO.raiseError[Unit](new RuntimeException("Expected exception")), IO.unit)
            .attempt
          _ <- ps.protect(id, ref.set(1), IO.unit)
          _ <- ps.protect(id, ref.set(3), IO.unit)
          result <- ref.get
        } yield {
          assertEquals(result, 1)
        }
      }
  }

  // TODO this test is ignored because it is the symptoms of the issue we have with this library
  test("Deduplication should not re-process multiple event if it failed the first time".ignore) {

    val processorId = given[UUID]
    val id = given[UUID]

    deduplicationResource(processorId, 1.seconds)
      .use { ps =>
        for {
          ref <- Ref[IO].of(0)
          _ <- ps
            .protect(id, IO.raiseError[Unit](new RuntimeException("Expected exception")), IO.unit)
            .attempt
          _ <- List
            .fill(100)(id)
            .parTraverse { _ =>
              ps.protect(id, ref.update(_ + 1), IO.unit)
            }
          result <- ref.get
        } yield {
          assertEquals(result, 1)
        }
      }
  }

  test("Deduplication should process the second event after the first one timeout") {

    val processorId = given[UUID]
    val id = given[UUID]

    val maxProcessingTime = 1.seconds
    deduplicationResource(processorId, maxProcessingTime)
      .use { ps =>
        for {
          _ <- ps.tryStartProcess(id)
          _ <- IO.sleep(maxProcessingTime + 1.second)
          result <- ps.tryStartProcess(id)
        } yield {
          assert(clue(result).isInstanceOf[Outcome.New[IO]])
        }
      }
  }

  test("Deduplication should fail with timeout if maxPoll < maxProcessingTime") {

    val processorId = given[UUID]
    val id = given[UUID]

    val maxProcessingTime = 10.seconds
    val maxPollingtime = 1.second

    deduplicationResource(processorId, maxProcessingTime, maxPollingtime)
      .use { ps =>
        for {
          _ <- ps.tryStartProcess(id)
          result <- ps.tryStartProcess(id).attempt
        } yield {
          assert(clue(result).isLeft)
          assert(result.left.exists(_.isInstanceOf[TimeoutException]))
        }
      }

  }

  test("Deduplication should process only one event out of multiple concurrent events") {

    val processorId = given[UUID]
    val id = given[UUID]

    val n = 120

    deduplicationResource(processorId, maxProcessingTime = 30.seconds)
      .use { d =>
        List
          .fill(math.abs(n))(id)
          .parTraverse { i =>
            d.protect(i, IO(1), IO(0))
          }
          .map { xs =>
            assertEquals(xs.sum, 1)
          }
      }
  }

  def deduplicationResource(
      processorId: UUID,
      maxProcessingTime: FiniteDuration = 5.seconds,
      maxPollingTime: FiniteDuration = 15.seconds
  ): Resource[IO, Deduplication[IO, UUID, UUID]] =
    for {
      processRepo <- processRepoResource[IO]
      config = Config(
        processorId = processorId,
        maxProcessingTime = maxProcessingTime,
        ttl = 1.day.some,
        pollStrategy = PollStrategy.backoff(maxDuration = maxPollingTime)
      )
      deduplication <- Resource.eval(Deduplication[IO, UUID, UUID](processRepo, config))
    } yield deduplication

}
