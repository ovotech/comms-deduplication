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

object Deduplication {

  def nowF[F[_]: Functor: Clock] =
    Clock[F]
      .realTime(TimeUnit.MILLISECONDS)
      .map(Instant.ofEpochMilli)

  def processStatus[F[_]: Monad: Clock](
      maxProcessingTime: FiniteDuration
  )(p: Process[_, _]): F[ProcessStatus] =
    if (p.completedAt.isDefined) {
      ProcessStatus.Completed.pure[F].widen
    } else {
      nowF[F]
        .map { now =>
          /*
           * If the startedAt is:
           *  - In the past compared to expected finishing time the processed has timeout
           *  - In the future compared to expected finishing time present the process has started but not yet completed
           */
          val isTimeout = p.startedAt
            .plus(maxProcessingTime.toJava)
            .isBefore(now)

          if (isTimeout)
            ProcessStatus.Timeout
          else
            ProcessStatus.Started
        }
    }

}

class Deduplication[F[_]: Sync: Timer, Id, ProcessorId](repo: ProcessRepo[F, Id, ProcessorId], config: Config[ProcessorId]) {

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
  def tryStartProcess(id: Id): F[Outcome[F]] = {

    val pollStrategy = config.pollStrategy

    def doIt(
        startedAt: Instant,
        pollNo: Int,
        pollDelay: FiniteDuration
    ): F[Outcome[F]] = {

      def nextStep(ps: ProcessStatus): F[Outcome[F]] = ps match {
        case ProcessStatus.Started =>
          val totalDurationF = nowF[F]
            .map(now => (now.toEpochMilli - startedAt.toEpochMilli).milliseconds)

          // retry until it is either Completed or Timeout
          totalDurationF
            .map(td => td >= pollStrategy.maxPollDuration)
            .ifM(
              Sync[F].raiseError(new TimeoutException(s"Stop polling after ${pollNo} polls")),
              Timer[F].sleep(pollDelay) >> doIt(
                startedAt,
                pollNo + 1,
                config.pollStrategy.nextDelay(pollNo, pollDelay)
              )
            )
        case ProcessStatus.NotStarted | ProcessStatus.Timeout =>
          Outcome
            .New(nowF[F].flatMap(now => repo.completeProcess(id, config.processorId, now)))
            .pure[F]
            .widen[Outcome[F]]

        case ProcessStatus.Completed =>
          Monad[F].point(Outcome.Duplicate())
      }

      for {
        now <- nowF[F]
        processOpt <- repo.startProcessingUpdate(id, config.processorId, now)
        status <- processOpt
          .traverse(processStatus[F](config.maxProcessingTime))
          .map(_.getOrElse(ProcessStatus.NotStarted))
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
  def protect[A](id: Id, ifNew: F[A], ifDuplicate: F[A]): F[A] = {
    tryStartProcess(id)
      .flatMap {
        case Outcome.New(markAsComplete) =>
          ifNew.flatTap(_ => markAsComplete)
        case Outcome.Duplicate() =>
          ifDuplicate
      }
  }
}
