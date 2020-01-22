package com.ovoenergy.comms.deduplication

import scala.concurrent.duration._

import Config.TableName
import model._
case class Config[ProcessorID](
    tableName: TableName,
    processorId: ProcessorID,
    maxProcessingTime: FiniteDuration,
    ttl: FiniteDuration,
    pollStrategy: PollStrategy
)

object Config {

  case class TableName(value: String)

}
