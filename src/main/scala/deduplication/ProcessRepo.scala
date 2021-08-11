package com.ovoenergy.comms.deduplication

import java.time.Instant

import model._
import scala.concurrent.duration.FiniteDuration

object ProcessRepo {
  sealed trait AttemptResult
  case object AttemptSucceded extends AttemptResult
  case object AttemptFailed extends AttemptResult
}

trait ProcessRepo[F[_], ID, ProcessorID, A] {

  def create(
      id: ID,
      processorId: ProcessorID,
      now: Instant
  ): F[Option[Process[ID, ProcessorID, A]]]

  def markAsCompleted(
      id: ID,
      processorId: ProcessorID,
      value: A,
      now: Instant,
      ttl: Option[FiniteDuration]
  ): F[Unit]

  def get(
      id: ID,
      processorId: ProcessorID
  ): F[Option[Process[ID, ProcessorID, A]]]

  def attemptReplacing(
      id: ID,
      processorId: ProcessorID,
      oldStartedAt: Instant,
      newStartedAt: Instant
  ): F[ProcessRepo.AttemptResult]

}
