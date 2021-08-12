package com.ovoenergy.comms.deduplication

import java.time.Instant

object model {

  /**
    * The outcome of starting a process.
    *
    * It ould be either New or Duplicate. The New has a markAsComplete member
    * that should be used to mark the process as complete after it has succeeded
    */
  sealed trait Outcome[F[_]]
  object Outcome {
    case class Duplicate[F[_]]() extends Outcome[F]
    case class New[F[_]](markAsComplete: F[Unit]) extends Outcome[F]
  }

  sealed trait ProcessStatus[A]
  object ProcessStatus {
    case class NotStarted[A]() extends ProcessStatus[A]
    case class Running[A]() extends ProcessStatus[A]
    case class Completed[A](a: A) extends ProcessStatus[A]
    case class Timeout[A](oldStartedAt: Instant) extends ProcessStatus[A]
    case class Expired[A](oldStartedAt: Instant) extends ProcessStatus[A]
  }

  case class Expiration(instant: Instant)
  case class Process[ID, ContextID, A](
      id: ID,
      contextId: ContextID,
      startedAt: Instant,
      expiresOn: Option[Instant],
      result: Option[A]
  )
}
