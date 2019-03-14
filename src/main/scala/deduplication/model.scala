package com.ovoenergy.comms.deduplication

import java.time.Instant
import scala.compat.java8.DurationConverters._

import scala.concurrent.duration._

import cats.implicits._

object model {

  sealed trait Status

  object Status {

    case object Processing extends Status

    case object Processed extends Status

    val processing: Status = Processing
    val processed: Status = Processed

    def fromString(s: String): Option[Status] = s.toLowerCase match {
      case "processing" => Processing.some
      case "processed" => Processed.some
      case _ => none[Status]
    }

    def unsafeFromString(s: String): Status =
      fromString(s)
        .getOrElse(throw new IllegalArgumentException(s"$s is not a valid Status"))
  }

  case class Expiration(instant: Instant) {

    def plus(d: FiniteDuration): Expiration =
      copy(instant.plus(d.toJava))
  }

  case class Process[ID, ProcessorID](
      id: ID,
      processorId: ProcessorID,
      status: Status,
      expiredOn: Option[Expiration])
}
