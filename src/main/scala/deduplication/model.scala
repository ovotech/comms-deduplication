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

  sealed trait ProcessStatus
  object ProcessStatus {
    case object NotStarted extends ProcessStatus
    case object Running extends ProcessStatus
    case object Completed extends ProcessStatus
    case object Timeout extends ProcessStatus
    case object Expired extends ProcessStatus
  }

  case class Expiration(instant: Instant)
  case class Process[ID, ProcessorID](
      id: ID,
      processorId: ProcessorID,
      startedAt: Instant,
      completedAt: Option[Instant],
      expiresOn: Option[Expiration]
  )
}
