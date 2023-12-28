package com.ovoenergy.comms.deduplication.meteor

import cats.effect.unsafe.implicits.global
import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.DeduplicationTestUtils._
import com.ovoenergy.comms.deduplication.meteor.model.EncodedResult
import java.time.Instant
import meteor.syntax._
import munit._
import scala.concurrent.duration._

class MeteorProcessRepoSuite extends FunSuite {

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  test("should segregate processes by context") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        contextId1 <- uuidF.map(_.toString())
        contextId2 <- uuidF.map(_.toString())
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        later = Instant.ofEpochMilli(now.toEpochMilli + 1000)
        testResult = EncodedResult("testresult".asAttributeValue)
        _ <- repo.create(id, contextId1, now)
        _ <- repo.create(id, contextId2, later)
        _ <- repo.markAsCompleted(id, contextId2, testResult, later, none)
        process1 <- repo.get(id, contextId1)
        process2 <- repo.get(id, contextId2)
      } yield {
        assertEquals(clue(process1).isDefined, true)
        assertEquals(clue(process2).isDefined, true)
        assert(clue(process1).exists(_.startedAt.equals(clue(now))))
        assert(clue(process2).exists(_.startedAt.equals(clue(later))))
        assert(clue(process2).exists(_.result.contains(testResult)))
      }
    }
  }

  test("`create` should add the record in dynamo") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
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
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        later = Instant.ofEpochMilli(now.toEpochMilli + 1000)
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
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        later = Instant.ofEpochMilli(now.toEpochMilli + 1000)
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
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        later = Instant.ofEpochMilli(now.toEpochMilli + 1000)
        testResult = EncodedResult("testresult".asAttributeValue)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(id, contextId, testResult, now, 10.seconds.some)
        process <- repo.get(id, contextId)
      } yield {
        val expectedExpiration = Instant.ofEpochMilli(now.toEpochMilli + 10000)
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.result.contains(testResult)))
        assert(clue(process).exists(_.expiresOn.contains(clue(expectedExpiration))))
      }
    }
  }

  test("`markAsCompleted` should be able to store a result with no expiration") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        later = Instant.ofEpochMilli(now.toEpochMilli + 1000)
        testResult = EncodedResult("testresult".asAttributeValue)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(id, contextId, testResult, now, none)
        process <- repo.get(id, contextId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.result.contains(testResult)))
        assert(clue(process).exists(_.expiresOn.isEmpty))
      }
    }
  }

  test(
    "`attemptReplacing` should replace the `startedAt` field and remove `result` and `expiresOn`"
  ) {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        later = Instant.ofEpochMilli(now.toEpochMilli + 1000)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(
          id,
          contextId,
          EncodedResult("123".asAttributeValue),
          now,
          30.days.some
        )
        _ <- repo.attemptReplacing(id, contextId, now, later)
        process <- repo.get(id, contextId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(later))))
        assert(clue(process).exists(_.result.isEmpty))
        assert(clue(process).exists(_.expiresOn.isEmpty))
      }
    }
  }

  test("`attemptReplacing` should only update the process if the `startedAt` field did not change") {
    testRepo.use { repo =>
      for {
        id <- uuidF.map(_.toString())
        contextId <- uuidF.map(_.toString())
        now <- Clock[IO].realTime.map(d => Instant.ofEpochMilli(d.toMillis))
        later = Instant.ofEpochMilli(now.toEpochMilli + 1000)
        testResult = EncodedResult("testresult".asAttributeValue)
        _ <- repo.create(id, contextId, now)
        _ <- repo.markAsCompleted(id, contextId, testResult, now, none)
        _ <- repo.attemptReplacing(id, contextId, later, later)
        process <- repo.get(id, contextId)
      } yield {
        assertEquals(clue(process).isDefined, true)
        assert(clue(process).exists(_.startedAt.equals(clue(now))))
        assert(clue(process).exists(_.result.contains(testResult)))
      }
    }
  }

}
