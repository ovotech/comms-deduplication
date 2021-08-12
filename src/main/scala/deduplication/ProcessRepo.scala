package com.ovoenergy.comms.deduplication

import java.time.Instant

import model._
import scala.concurrent.duration.FiniteDuration

object ProcessRepo {
  sealed trait AttemptResult
  case object AttemptSucceded extends AttemptResult
  case object AttemptFailed extends AttemptResult
}

trait ProcessRepo[F[_], ID, ContextID, A] {

  def create(
      id: ID,
      contextId: ContextID,
      now: Instant
  ): F[Option[Process[ID, ContextID, A]]]

  def markAsCompleted(
      id: ID,
      contextId: ContextID,
      value: A,
      now: Instant,
      ttl: Option[FiniteDuration]
  ): F[Unit]

  def get(
      id: ID,
      contextId: ContextID
  ): F[Option[Process[ID, ContextID, A]]]

  def attemptReplacing(
      id: ID,
      contextId: ContextID,
      oldStartedAt: Instant,
      newStartedAt: Instant
  ): F[ProcessRepo.AttemptResult]

}
