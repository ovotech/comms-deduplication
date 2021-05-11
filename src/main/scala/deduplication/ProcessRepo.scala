package com.ovoenergy.comms.deduplication

import java.time.Instant

import model._
import scala.concurrent.duration.FiniteDuration

trait ProcessRepo[F[_], ID, ProcessorID] {

  def startProcessingUpdate(
      id: ID,
      processorId: ProcessorID,
      now: Instant
  ): F[Option[Process[ID, ProcessorID]]]

  def completeProcess(
      id: ID,
      processorId: ProcessorID,
      now: Instant,
      ttl: Option[FiniteDuration]
  ): F[Unit]

  def invalidateProcess(
      id: ID,
      processorId: ProcessorID
  ): F[Unit]
}
