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
      now: Instant,
      overrideStartedAt: Option[Instant] = none
  )(p: Process[_, _]): ProcessStatus = {

    val isCompleted =
      p.completedAt.isDefined

    val isExpired = p.expiresOn
      .map(_.instant)
      .exists(_.isBefore(now))

    val isTimeout = overrideStartedAt
      .getOrElse(p.startedAt)
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

  /**
    * Try to start a process.
    *
    * If the process with the giving id has never started before, this will start a new process and return Outcome.New
    * If another process with the same id has already completed, this will do nothing and return Outcome.Duplicate
    * If another process with the same id has already started and timeouted, this will start a new process and return Outcome.New
    * If another process with the same id is still running, this will wait until it will complete or timeout and return Outcome.Duplicate or Outcome.New
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

    /* 1 - Write the process to the DB
     * 2 - The process does not exist -> start
     *     The process is completed but is expired -> start
     *     The process is completed -> skip
     *     The process is not completed and timeout (now - previous.startedAt > maxProcessingTime) -> start
     *     The process is not completed neither timeout -> retry in n millis
     */

    def loop(
        startedAt: Instant,
        pollNo: Int,
        pollDelay: FiniteDuration,
        firstProcess: Option[Process[ID, ProcessorID]]
    ): F[Outcome[F]] = {
      def logContext =
        s"processorId=${config.processorId}, id=${id}, startedAt=${startedAt}, pollNo=${pollNo}"
      for {
        now <- nowF[F]
        optPrevProcess <- repo.startProcessingUpdate(id, config.processorId, startedAt)
        optStatus = optPrevProcess.map(
          processStatus(
            config.maxProcessingTime,
            now,
            overrideStartedAt = firstProcess.map(_.startedAt)
          )
        )
        outcome <- optStatus match {
          case None =>
            logger.debug(s"No previous process found. Starting now. ${logContext}") >>
              startProcess(id)
          case Some(ProcessStatus.Expired) =>
            logger.debug(s"Previous process is expired. Starting now. ${logContext}") >>
              startProcess(id)
          case Some(ProcessStatus.Timeout) =>
            logger.debug(s"Previous process is timed out. Starting now. ${logContext}") >>
              startProcess(id)
          case Some(ProcessStatus.Completed) =>
            logger.debug(s"Previous process has completed. Not doing anything. ${logContext}") >>
              skipProcess
          case Some(ProcessStatus.Running)
              if (now.toEpochMilli - startedAt.toEpochMilli).milliseconds <= pollStrategy.maxPollDuration =>
            logger.debug(s"Previous process is running. Waiting... ${logContext}") >>
              Timer[F].sleep(pollDelay) >>
              loop(
                startedAt,
                pollNo + 1,
                pollStrategy.nextDelay(pollNo, pollDelay),
                firstProcess.orElse(optPrevProcess)
              )
          case Some(_) =>
            logger.debug(s"Exceeded maxPollDuration. Stopping. ${logContext}") >>
              Sync[F]
                .raiseError[Outcome[F]](new TimeoutException(s"Stop polling after ${pollNo} polls"))
        }

      } yield outcome
    }

    nowF[F].flatMap(now => loop(now, 0, pollStrategy.initialDelay, none))
  }

  private def startProcess(id: ID): F[Outcome[F]] = {
    Outcome
      .neu(
        nowF[F].flatMap { now =>
          repo.completeProcess(id, config.processorId, now, config.ttl).flatTap { _ =>
            logger.debug(
              s"Process marked as completed. processorId=${config.processorId}, id=${id}"
            )
          }
        }
      )
      .pure[F]
  }

  private def skipProcess: F[Outcome[F]] = Outcome.duplicate[F].pure[F]

}
