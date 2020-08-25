package com.ovoenergy.comms.deduplication

import java.{util => ju}
import java.time.Instant

import scala.concurrent.duration._

import cats.implicits._
import cats.effect._
import cats.effect.laws.util._

import org.scalacheck.Prop._

import model._
import Generators._

class ProcessStatusSuite extends munit.ScalaCheckSuite {

  property(
    "processStatus should return Completed if the completedAt is present and expiredOn is None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        completedAt = now.some,
        expiresOn = None
      )

      val status = Deduplication.processStatus(maxProcessingTime, now)(sample)

      assertEquals(clue(status), ProcessStatus.Completed)
    }
  }

  property(
    "processStatus should return Completed if the completedAt is present and expiredOn is in the future"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        completedAt = now.some,
        expiresOn = Expiration(now.plusMillis(1)).some
      )

      val status = Deduplication.processStatus(maxProcessingTime, now)(sample)

      assertEquals(clue(status), ProcessStatus.Completed)
    }
  }

  property("processStatus should return Expired if the expiresOn is present and it is in the past") {
    forAll { process: Process[ju.UUID, ju.UUID] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        expiresOn = Expiration(now.minusMillis(1)).some
      )

      val status = Deduplication.processStatus(maxProcessingTime, now)(sample)

      assertEquals(clue(status), ProcessStatus.Expired)
    }
  }

  property(
    "processStatus should return Timeout if startedAt is more than maxProcessingTime in the past, completedAt and expiresOn are None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).minusMillis(1),
        completedAt = None,
        expiresOn = None
      )

      val status = Deduplication.processStatus(maxProcessingTime, now)(sample)

      assertEquals(clue(status), ProcessStatus.Timeout)
    }
  }

  property(
    "processStatus should return Timeout if startedAt is more than maxProcessingTime in the past, expiresOn is in the future and completedAt is None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).minusMillis(1),
        completedAt = None,
        expiresOn = Expiration(now.plusMillis(1)).some
      )

      val status = Deduplication.processStatus(maxProcessingTime, now)(sample)

      assertEquals(clue(status), ProcessStatus.Timeout)
    }
  }

  property(
    "processStatus returns running if startedAt is less than maxProcessingTime in the past, completedAt and expiresOn are None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).plusMillis(1),
        completedAt = None,
        expiresOn = None
      )

      val status = Deduplication.processStatus(maxProcessingTime, now)(sample)

      assertEquals(clue(status), ProcessStatus.Running)
    }
  }

  property(
    "processStatus returns running if startedAt is less than maxProcessingTime in the past,  expiresOn is in the future and completedAt is None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).plusMillis(1),
        completedAt = None,
        expiresOn = Expiration(now.plusMillis(1)).some
      )

      val status = Deduplication.processStatus(maxProcessingTime, now)(sample)

      assertEquals(clue(status), ProcessStatus.Running)
    }
  }
}
