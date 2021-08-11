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
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

trait Deduplication[F[_], ID, ProcessorID, A] {

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
  def protect(id: ID, process: F[A]): F[A]
}

object Deduplication {

  def nowF[F[_]: Functor: Clock] =
    Clock[F]
      .realTime(TimeUnit.MILLISECONDS)
      .map(Instant.ofEpochMilli)

  def processStatus[A](
      maxProcessingTime: FiniteDuration,
      now: Instant
  )(p: Option[Process[_, _, A]]): ProcessStatus[A] = {

    p.fold[ProcessStatus[A]](ProcessStatus.NotStarted()) { p =>
      val isExpired = p.expiresOn
        .exists(_.isBefore(now))

      val isTimeout = p.startedAt
        .plus(maxProcessingTime.toJava)
        .isBefore(now)

      if (isExpired) {
        ProcessStatus.Expired(p.startedAt)
      } else if (isTimeout) {
        ProcessStatus.Timeout(p.startedAt)
      } else {
        p.result match {
          case Some(result) => ProcessStatus.Completed(result)
          case None => ProcessStatus.Running()
        }
      }
    }
  }

  def apply[F[_]: Sync: Timer, ID, ProcessorID, A](
      processRepo: ProcessRepo[F, ID, ProcessorID, A],
      config: Config[ProcessorID]
  ): F[Deduplication[F, ID, ProcessorID, A]] = Slf4jLogger.create[F].map { logger =>
    new Deduplication[F, ID, ProcessorID, A] {

      override def protect(id: ID, fa: F[A]): F[A] =
        for {
          now <- nowF[F]
          existingProcess <- processRepo.create(id, config.processorId, Instant.now())
          result <- handleScenarios(id, fa, existingProcess, now, config.pollStrategy.initialDelay)
        } yield result

      private def handleScenarios(
          id: ID,
          fa: F[A],
          existingProcess: Option[Process[ID, ProcessorID, A]],
          pollingStartedAt: Instant,
          pollDelay: FiniteDuration,
          attemptNumber: Int = 1
      ): F[A] = {

        def waitAndRetry: F[A] =
          for {
            _ <- Timer[F].sleep(pollDelay)
            updatedProcess <- processRepo.get(id, config.processorId)
            nextDelay = config.pollStrategy.nextDelay(attemptNumber, pollDelay)
            result <- handleScenarios(
              id,
              fa,
              updatedProcess,
              pollingStartedAt,
              nextDelay,
              attemptNumber + 1
            )
          } yield result

        def attemptReplacingProcesss(oldStartedAt: Instant, newStartedAt: Instant): F[A] =
          processRepo
            .attemptReplacing(id, config.processorId, oldStartedAt, newStartedAt)
            .flatMap {
              case ProcessRepo.AttemptSucceded => runProcess(id, fa)
              case ProcessRepo.AttemptFailed => waitAndRetry
            }

        def logContext: String =
          s"processorId=${config.processorId}, id=${id}, startedAt=${pollingStartedAt}, pollNo=${attemptNumber}"

        val stopRetry: F[A] =
          logger.warn(
            s"Process still running, stop retry-ing ${logContext}"
          ) >> Sync[F]
            .raiseError[A](new TimeoutException(s"Stop polling after ${attemptNumber} polls"))

        for {
          now <- nowF[F]
          totalDuration = (now.toEpochMilli - pollingStartedAt.toEpochMilli).milliseconds
          _ <- if (totalDuration >= config.pollStrategy.maxPollDuration) stopRetry else Sync[F].unit
          status = processStatus[A](config.maxProcessingTime, now)(existingProcess)
          result <- status match {
            case ProcessStatus.NotStarted() => runProcess(id, fa)
            case ProcessStatus.Completed(result) => result.pure[F]
            case ProcessStatus.Running() => waitAndRetry
            case ProcessStatus.Timeout(oldStartedAt) => attemptReplacingProcesss(oldStartedAt, now)
            case ProcessStatus.Expired(oldStartedAt) => attemptReplacingProcesss(oldStartedAt, now)
          }
        } yield result
      }

      private def runProcess(id: ID, fa: F[A]): F[A] =
        for {
          result <- fa
          now <- nowF[F]
          _ <- processRepo.markAsCompleted(id, config.processorId, result, now, config.ttl)
        } yield result
    }
  }

}
