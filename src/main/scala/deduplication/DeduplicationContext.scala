package com.ovoenergy.comms.deduplication

import cats._
import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.model._
import io.chrisdavenport.log4cats.Logger
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration._

trait DeduplicationContext[F[_], ID, ContextID, A] {

  def contextId: ContextID

  /**
    * Do the best effort to ensure a process to be successfully executed only once.
    *
    * If the process has already runned successfully before, it will return the original result of
    * [[fa]]
    * Otherwise, it will run [[fa]].
    *
    * @param id The id of the process to run.
    * @param fa The effect to run if the process is new.
    * @return the result of [[fa]]
    */
  def protect(id: ID, fa: F[A]): F[A]
}

object DeduplicationContext {

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

  def apply[F[_]: Sync: Timer, ID, ContextID, A](
      id: ContextID,
      processRepo: ProcessRepo[F, ID, ContextID, A],
      config: Config,
      logger: Logger[F]
  ): DeduplicationContext[F, ID, ContextID, A] =
    new DeduplicationContext[F, ID, ContextID, A] {

      val contextId = id

      override def protect(id: ID, fa: F[A]): F[A] =
        for {
          now <- nowF[F]
          existingProcess <- processRepo.create(id, contextId, Instant.now())
          result <- handleScenarios(id, fa, existingProcess, now, config.pollStrategy.initialDelay)
        } yield result

      private def handleScenarios(
          id: ID,
          fa: F[A],
          existingProcess: Option[Process[ID, ContextID, A]],
          pollingStartedAt: Instant,
          pollDelay: FiniteDuration,
          attemptNumber: Int = 1
      ): F[A] = {

        def waitAndRetry: F[A] =
          for {
            _ <- Timer[F].sleep(pollDelay)
            updatedProcess <- processRepo.get(id, contextId)
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
            .attemptReplacing(id, contextId, oldStartedAt, newStartedAt)
            .flatMap {
              case ProcessRepo.AttemptSucceded => runProcess(id, fa)
              case ProcessRepo.AttemptFailed => waitAndRetry
            }

        def logContext: String =
          s"contextId=${contextId}, id=${id}, startedAt=${pollingStartedAt}, pollNo=${attemptNumber}"

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
          _ <- processRepo.markAsCompleted(id, contextId, result, now, config.ttl)
        } yield result
    }

}
