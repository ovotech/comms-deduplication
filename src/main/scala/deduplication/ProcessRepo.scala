package com.ovoenergy.comms.deduplication

import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

object ProcessRepo {
  sealed trait AttemptResult
  case object AttemptSucceded extends AttemptResult
  case object AttemptFailed extends AttemptResult
}

/**
  * An interface for a data backend used by Deduplication.
  *
  * It will store [[Process]] objects, identified by [[id]] and [[contextId]]
  */
trait ProcessRepo[F[_], ID, ContextID, A] {

  /**
    * Try to create a new process entry in the repo.
    *
    * It should only store it if the process is new, otherwise it should return the existing process
    * and not change anything
    *
    * @param id The id of the process that is about to start
    * @param contextId The context of the process that is about to start
    * @param now An [[Instant]] representing when the process is starting
    * @return None if the process is new, otherwise the old existing process
    */
  def create(
      id: ID,
      contextId: ContextID,
      now: Instant
  ): F[Option[Process[ID, ContextID, A]]]

  /**
    * Mark a process as completed and store its result
    *
    * @param id The id of the process
    * @param contextId The context of the process
    * @param value The result of the process execution
    *
    * @param now An [[Instant]] for the current time, used to calculate the expireAt value
    * @param ttl How long the process entry should be considered valid
    */
  def markAsCompleted(
      id: ID,
      contextId: ContextID,
      value: A,
      now: Instant,
      ttl: Option[FiniteDuration]
  ): F[Unit]

  /**
    * Retrieve a process entry from the repo
    *
    * @param id The id of the process
    * @param contextId The context of the process
    * @return The process if present, None otherwise
    */
  def get(
      id: ID,
      contextId: ContextID
  ): F[Option[Process[ID, ContextID, A]]]

  /**
    * Try to replace a process entry in the repo.
    *
    * This operation should only succeed if [[oldStartedAt]] is the same as the 'startedAt' field in
    * the existing process. If there is a mismatch the operation should fail.
    *
    * If the operation succeeds, the 'startedAt' field of the Process should be updated and the
    * 'result' and 'expiresOn' fields deleted.
    *
    * @param id The id of the process
    * @param contextId The context of the process
    * @param oldStartedAt the 'startedAt' value of the existing process, as it was last seen by
    *                     Deduplication.
    * @param newStartedAt the value the `startedAt` field should be set to if the operation succeeds
    * @return [[ProcessRepo.AttemptSucceded]] if the operation was successful.
    *         [[ProcessRepo.AttemptFailed]] otherwise
    */
  def attemptReplacing(
      id: ID,
      contextId: ContextID,
      oldStartedAt: Instant,
      newStartedAt: Instant
  ): F[ProcessRepo.AttemptResult]

}
