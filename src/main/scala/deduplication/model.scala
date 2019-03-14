package com.ovoenergy.comms.deduplication

import java.time.Instant

import scala.concurrent.duration._
import scala.compat.java8.DurationConverters._

import cats.implicits._

object model {

  case class Expiration(instant: Instant)
  case class Process[ID, ProcessorID](
      id: ID,
      processorId: ProcessorID,
      startedAt: Instant,
      completedAt: Option[Instant],
      expiresOn: Option[Expiration]
  )
}
