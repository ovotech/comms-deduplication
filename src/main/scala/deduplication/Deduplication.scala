package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import scala.concurrent.duration._
import scala.compat.java8.DurationConverters._

import cats._
import cats.implicits._
import cats.effect._

import com.ovoenergy.comms.deduplication.model._
import Deduplication._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.chrisdavenport.log4cats.StructuredLogger

object Deduplication {

  def nowF[F[_]: Functor: Clock] =
    Clock[F]
      .realTime(TimeUnit.MILLISECONDS)
      .map(Instant.ofEpochMilli)

  def processStatus(
      maxProcessingTime: FiniteDuration,
      now: Instant
  )(p: Process[_, _]): ProcessStatus = {

    val isCompleted =
      p.completedAt.isDefined

    val isExpired = p.expiresOn
      .map(_.instant)
      .exists(_.isBefore(now))

    val isTimeout = p.startedAt
      .plus(maxProcessingTime.toJava)
      .isBefore(now)

    if (isExpired) {
      ProcessStatus.Expired
    } else if (isCompleted) {
      ProcessStatus.Completed
    } else if (isTimeout) {
      ProcessStatus.Timeout
    } else {
      ProcessStatus.Running
    }
  }

  def apply[F[_]: Sync: Timer, ID, ProcessorID](
      repo: ProcessRepo[F, ID, ProcessorID],
      config: Config[ProcessorID]
  ): F[Deduplication[F, ID, ProcessorID]] = Slf4jLogger.create[F].map { logger =>
    new Deduplication[F, ID, ProcessorID](repo, config, logger)
  }

}

class Deduplication[F[_]: Sync: Timer, ID, ProcessorID] private (
    repo: ProcessRepo[F, ID, ProcessorID],
    config: Config[ProcessorID],
    logger: StructuredLogger[F]
) {

  /**
    * Try to start a process.
    *
    * If the process with the giving id has never started before, this will start a new process and return Some(complete)
    * If another process with the same id has already completed, this will do nothing and return None
    * If another process with the same id has already started and timeouted, this will do nothing and return None
    * If another process with the same id is still running, this will wait until it will complete or timeout
    *
    * If markAsComplete fails, the process will likely be duplicated.
    * If the process takes more time than maxProcessingTime, you may have duplicate if two processes with same ID happen at the same time
    *
    * eg
    * ```
    * tryStartProcess(id)
    *   .flatMap {
    *     case Outcome.New(markAsComplete) =>
    *       doYourStuff.flatTap(_ => markAsComplete)
    *     case Outcome.Duplicate() =>
    *       dontDoYourStuff
    *   }
    * ```
    *
    * @param id The process id to start
    * @return An Outcome.New or Outcome.Duplicate. The Outcome.New will contain an effect to complete the just started process.
    */
  def tryStartProcess(id: ID): F[Outcome[F]] = {

    val pollStrategy = config.pollStrategy

    def doIt(
        startedAt: Instant,
        pollNo: Int,
        pollDelay: FiniteDuration
    ): F[Outcome[F]] = {

      def logContext =
        s"processorId=${config.processorId}, id=${id}, startedAt=${startedAt}, pollNo=${pollNo}"

      def nextStep(ps: ProcessStatus): F[Outcome[F]] = ps match {
        case ProcessStatus.Running =>
          val totalDurationF = nowF[F]
            .map(now => (now.toEpochMilli - startedAt.toEpochMilli).milliseconds)

          // retry until it is either Completed or Timeout
          totalDurationF
            .map(td => td >= pollStrategy.maxPollDuration)
            .ifM(
              logger.warn(
                s"Process still running, stop retry-ing ${logContext}"
              ) >> Sync[F].raiseError(new TimeoutException(s"Stop polling after ${pollNo} polls")),
              logger.debug(
                s"Process still running, retry-ing ${logContext}"
              ) >>
                Timer[F].sleep(pollDelay) >>
                doIt(
                  startedAt,
                  pollNo + 1,
                  config.pollStrategy.nextDelay(pollNo, pollDelay)
                )
            )
        case ProcessStatus.NotStarted | ProcessStatus.Timeout | ProcessStatus.Expired =>
          logger
            .debug(
              s"Process status is ${ps}, starting now ${logContext}"
            )
            .map { _ =>
              Outcome.New(
                nowF[F].flatMap { now =>
                  repo.completeProcess(id, config.processorId, now, config.ttl).flatTap { _ =>
                    logger.debug(
                      s"Process marked as completed ${logContext}"
                    )
                  }
                }
              )
            }
            .widen[Outcome[F]]

        case ProcessStatus.Completed =>
          logger
            .debug(
              s"Process is duplicated processorId=${config.processorId}, id=${id}, startedAt=${startedAt}"
            )
            .as(Outcome.Duplicate())
      }

      for {
        now <- nowF[F]
        processOpt <- repo.startProcessingUpdate(id, config.processorId, now)
        status = processOpt
          .fold[ProcessStatus](ProcessStatus.NotStarted) { p =>
            processStatus(config.maxProcessingTime, now)(p)
          }
        sample <- nextStep(status)
      } yield sample
    }

    nowF[F].flatMap(now => doIt(now, 0, pollStrategy.initialDelay))
  }

  /**
    * Do the best effort to ensure a process to be successfully executed only once.
    *
    * If the process has already runned successfully before, it will run the [[ifDuplicate]].
    * Otherwise, it will run the [[ifNew]].
    *
    * The return value is either the result of [[ifNew]] or [[ifDuplicate]].
    *
    * @param id The id of the process to run.
    * @param ifNew The effect to run if the process is new.
    * @param ifDuplicate The effect to run if the process is duplicate.
    * @return the result of [[ifNew]] or [[ifDuplicate]].
    */
  def protect[A](id: ID, ifNew: F[A], ifDuplicate: F[A]): F[A] = {
    tryStartProcess(id)
      .flatMap {
        case Outcome.New(markAsComplete) =>
          ifNew.flatTap(_ => markAsComplete)
        case Outcome.Duplicate() =>
          ifDuplicate
      }
  }
}
