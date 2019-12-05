package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._

import cats._
import cats.implicits._
import cats.effect._

object model {

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
