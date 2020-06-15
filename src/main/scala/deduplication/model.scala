package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import com.ovoenergy.comms.deduplication.model.Sample.NotSeen
import com.ovoenergy.comms.deduplication.model.Sample.Seen

object model {

  sealed trait Sample {
    def fold[A](noSeen: => A, seen: => A) = this match {
      case NotSeen => noSeen
      case Seen => seen
    }
  }

  object Sample {

    val seen: Sample = Seen
    val notSeen: Sample = NotSeen

    case object Seen extends Sample
    case object NotSeen extends Sample
  }

  sealed trait ProcessStatus
  object ProcessStatus {
    case object NotStarted extends ProcessStatus
    case object Started extends ProcessStatus
    case object Completed extends ProcessStatus
    case object Expired extends ProcessStatus
  }

  case class Expiration(instant: Instant)
  case class Process[ID, ProcessorID](
      id: ID,
      processorId: ProcessorID,
      startedAt: Instant,
      completedAt: Option[Instant],
      expiresOn: Option[Expiration]
  )

  trait PollStrategy {
    def maxPollDuration: FiniteDuration
    def initialDelay: FiniteDuration
    def nextDelay(pollNo: Int, previousDelay: FiniteDuration): FiniteDuration
  }

  object PollStrategy {

    def linear(
        delay: FiniteDuration = 50.milliseconds,
        maxDuration: FiniteDuration = 3.seconds
    ) = new PollStrategy {
      def maxPollDuration = maxDuration
      def initialDelay = delay
      def nextDelay(pollNo: Int, previousDelay: FiniteDuration) = delay
    }

    def backoff(
        baseDelay: FiniteDuration = 50.milliseconds,
        multiplier: Double = 1.5d,
        maxDuration: FiniteDuration = 3.seconds
    ) = new PollStrategy {
      def maxPollDuration = maxDuration
      def initialDelay = baseDelay
      def nextDelay(pollNo: Int, previousDelay: FiniteDuration) =
        FiniteDuration((previousDelay.toMillis * multiplier).toLong, TimeUnit.MILLISECONDS)
    }
  }
}
