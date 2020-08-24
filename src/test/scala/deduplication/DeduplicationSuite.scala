package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.{util => ju}
import java.util.UUID

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import cats.implicits._
import cats.effect._
import cats.effect.laws.util.TestContext

import munit._
import org.scalacheck.Arbitrary

import model._
import Config._

class DeduplicationSuite extends FunSuite {

  private val testContext = TestContext()

  implicit val ec: ExecutionContext = testContext
  implicit val contextShift: ContextShift[IO] = testContext.ioContextShift
  implicit val timer: Timer[IO] = testContext.ioTimer

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  def given[A: Arbitrary]: A = Arbitrary.arbitrary[A].sample.get

  class MockProcessRepo extends ProcessRepo[IO, UUID, UUID] {
    def startProcessingUpdate(
        id: UUID,
        processorId: ju.UUID,
        now: Instant
    ): IO[Option[Process[UUID, UUID]]] = none[Process[UUID, UUID]].pure[IO]
    def completeProcess(id: UUID, processorId: UUID, now: Instant, ttl: FiniteDuration): IO[Unit] =
      IO.unit
  }

  test(
    "tryStartProcess should return New when the process with the given ID has never started before"
  ) {

    val processId = given[UUID]
    val processorId = given[UUID]

    val deduplication = Deduplication[IO, UUID, UUID](
      new MockProcessRepo {
        override def startProcessingUpdate(
            id: UUID,
            processorId: ju.UUID,
            now: Instant
        ): IO[Option[Process[UUID, UUID]]] = none[Process[UUID, UUID]].pure[IO]
      },
      Config(
        processorId = processorId,
        maxProcessingTime = 5.minutes,
        ttl = 30.days,
        pollStrategy = PollStrategy.linear()
      )
    )

    deduplication
      .flatMap { deduplication =>
        deduplication.tryStartProcess(processId)
      }
      .map { outcome =>
        assert(outcome.isInstanceOf[Outcome.New[IO]], clue(outcome))
      }

  }

  test(
    "tryStartProcess should return Duplicate when the process with the given ID has already completed"
  ) {

    val processId = given[UUID]
    val processorId = given[UUID]

    val deduplication = Deduplication[IO, UUID, UUID](
      new MockProcessRepo {
        override def startProcessingUpdate(
            id: UUID,
            processorId: ju.UUID,
            now: Instant
        ): IO[Option[Process[UUID, UUID]]] = {
          Deduplication.nowF[IO].map { now =>
            Process(
              id = id,
              processorId = processorId,
              startedAt = now.minusMillis(750),
              completedAt = now.minusMillis(250).some,
              expiresOn = None
            ).some
          }
        }
      },
      Config(
        processorId = processorId,
        maxProcessingTime = 5.minutes,
        ttl = 30.days,
        pollStrategy = PollStrategy.linear()
      )
    )

    deduplication
      .flatMap { deduplication =>
        deduplication.tryStartProcess(processId)
      }
      .map { outcome =>
        assertEquals(outcome, Outcome.Duplicate[IO]())
      }
  }

  test(
    "tryStartProcess should return New when another process with the same Id has never completed and expired"
  ) {

    val processId = given[UUID]
    val processorId = given[UUID]

    val deduplication = Deduplication[IO, UUID, UUID](
      new MockProcessRepo {
        override def startProcessingUpdate(
            id: UUID,
            processorId: ju.UUID,
            now: Instant
        ): IO[Option[Process[UUID, UUID]]] = {
          Deduplication.nowF[IO].map { now =>
            Process(
              id = id,
              processorId = processorId,
              startedAt = now.minusMillis(750),
              completedAt = None,
              expiresOn = Expiration(now.minusMillis(250)).some
            ).some
          }
        }
      },
      Config(
        processorId = processorId,
        maxProcessingTime = 5.minutes,
        ttl = 30.days,
        pollStrategy = PollStrategy.linear()
      )
    )

    deduplication
      .flatMap { deduplication =>
        deduplication.tryStartProcess(processId)
      }
      .map { outcome =>
        assert(outcome.isInstanceOf[Outcome.New[IO]], clue(outcome))
      }
  }

  test(
    "tryStartProcess should return New when another process with the same Id has completed and expired"
  ) {

    val processId = given[UUID]
    val processorId = given[UUID]

    val deduplication = Deduplication[IO, UUID, UUID](
      new MockProcessRepo {
        override def startProcessingUpdate(
            id: UUID,
            processorId: ju.UUID,
            now: Instant
        ): IO[Option[Process[UUID, UUID]]] = {
          Deduplication.nowF[IO].map { now =>
            Process(
              id = id,
              processorId = processorId,
              startedAt = now.minusMillis(750),
              completedAt = now.minusMillis(500).some,
              expiresOn = Expiration(now.minusMillis(250)).some
            ).some
          }
        }
      },
      Config(
        processorId = processorId,
        maxProcessingTime = 5.minutes,
        ttl = 30.days,
        pollStrategy = PollStrategy.linear()
      )
    )

    deduplication
      .flatMap { deduplication =>
        deduplication.tryStartProcess(processId)
      }
      .map { outcome =>
        assert(outcome.isInstanceOf[Outcome.New[IO]], clue(outcome))
      }
  }

  test(
    "tryStartProcess should return New when another process with the same Id has timeout"
  ) {

    val processId = given[UUID]
    val processorId = given[UUID]

    val deduplication = Deduplication[IO, UUID, UUID](
      new MockProcessRepo {
        override def startProcessingUpdate(
            id: UUID,
            processorId: ju.UUID,
            now: Instant
        ): IO[Option[Process[UUID, UUID]]] = {
          Deduplication.nowF[IO].map { now =>
            Process(
              id = id,
              processorId = processorId,
              startedAt = now.minusSeconds(300),
              completedAt = None,
              expiresOn = None
            ).some
          }
        }
      },
      Config(
        processorId = processorId,
        maxProcessingTime = 100.seconds,
        ttl = 30.days,
        pollStrategy = PollStrategy.linear()
      )
    )

    deduplication
      .flatMap { deduplication =>
        deduplication.tryStartProcess(processId)
      }
      .map { outcome =>
        assert(outcome.isInstanceOf[Outcome.New[IO]], clue(outcome))
      }
  }
}
