package com.ovoenergy.comms.deduplication

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import Config._

/**
  * Configure the library
  *
  * The [[PollStrategy]] controls the polling for waiting for a started process to complete or timeout. For
  * this reason is important for the pollStrategy.maxPollDuration to be > maxProcessingTime otherwise the poll
  * will always timeout in case of a stale process.
  *
  * @param maxProcessingTime
  * @param ttl
  * @param pollStrategy
  */
case class Config(
    maxProcessingTime: FiniteDuration,
    ttl: Option[FiniteDuration],
    pollStrategy: PollStrategy
)

object Config {

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
