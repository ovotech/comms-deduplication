package com.ovoenergy.comms.deduplication

import java.time.Instant

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
