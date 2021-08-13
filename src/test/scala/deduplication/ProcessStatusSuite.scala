package com.ovoenergy.comms.deduplication

import cats.implicits._
import com.ovoenergy.comms.deduplication.Generators._
import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import java.{util => ju}
import org.scalacheck.Prop._
import scala.concurrent.duration._

class ProcessStatusSuite extends munit.ScalaCheckSuite {

  property(
    "processStatus should return NotStarted if the Process is missing"
  ) {
    forAll { (now: Instant, maxProcessingTime: FiniteDuration) =>

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(
        none[Process[String, String, String]]
      )

      assertEquals(clue(status), ProcessStatus.NotStarted[String]())
    }
  }

  property(
    "processStatus should return Completed if the result is present and expiredOn is None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID, String] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        result = now.toString().some,
        expiresOn = None
      )

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(sample.some)

      assertEquals(clue(status), ProcessStatus.Completed(now.toString))
    }
  }

  property(
    "processStatus should return Completed if the result is present and expiredOn is in the future"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID, String] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        result = "res".some,
        expiresOn = now.plusMillis(1).some
      )

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(sample.some)

      assertEquals(clue(status), ProcessStatus.Completed("res"))
    }
  }

  property("processStatus should return Expired if the expiresOn is present and it is in the past") {
    forAll { process: Process[ju.UUID, ju.UUID, String] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val expiration = now.minusMillis(1)
      val sample = process.copy(
        expiresOn = expiration.some
      )

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(sample.some)

      assertEquals(clue(status), ProcessStatus.Expired[String](sample.startedAt))
    }
  }

  property(
    "processStatus should return Timeout if startedAt is more than maxProcessingTime in the past, result and expiresOn are None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID, String] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).minusMillis(1),
        result = none[String],
        expiresOn = None
      )

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(sample.some)

      assertEquals(clue(status), ProcessStatus.Timeout[String](sample.startedAt))
    }
  }

  property(
    "processStatus should return Timeout if startedAt is more than maxProcessingTime in the past, expiresOn is in the future and result is None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID, String] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).minusMillis(1),
        result = none[String],
        expiresOn = now.plusMillis(1).some
      )

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(sample.some)

      assertEquals(clue(status), ProcessStatus.Timeout[String](sample.startedAt))
    }
  }

  property(
    "processStatus returns running if startedAt is less than maxProcessingTime in the past, result and expiresOn are None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID, String] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).plusMillis(1),
        result = none[String],
        expiresOn = None
      )

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(sample.some)

      assertEquals(clue(status), ProcessStatus.Running[String]())
    }
  }

  property(
    "processStatus returns running if startedAt is less than maxProcessingTime in the past,  expiresOn is in the future and result is None"
  ) {
    forAll { process: Process[ju.UUID, ju.UUID, String] =>

      val now = Instant.now()
      val maxProcessingTime = 5.minutes

      val sample = process.copy(
        startedAt = now.minusMillis(maxProcessingTime.toMillis).plusMillis(1),
        result = none[String],
        expiresOn = now.plusMillis(1).some
      )

      val status = DeduplicationContext.processStatus(maxProcessingTime, now)(sample.some)

      assertEquals(clue(status), ProcessStatus.Running[String]())
    }
  }
}
