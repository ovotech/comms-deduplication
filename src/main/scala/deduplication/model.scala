package com.ovoenergy.comms.deduplication

import java.time.Instant

object model {

  sealed trait DeduplicationError
  object DeduplicationError {}

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

  sealed trait Result[A] {
    def value: A
    def isDuplicate: Boolean
  }
  case class New[A](value: A) extends Result[A] {
    val isDuplicate: Boolean = false
  }
  case class Duplicate[A](value: A) extends Result[A] {
    val isDuplicate: Boolean = true
  }
}
