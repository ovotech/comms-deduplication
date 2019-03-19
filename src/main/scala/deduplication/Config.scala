package com.ovoenergy.comms.deduplication

import scala.concurrent.duration._

import Config.TableName

case class Config[ProcessorID](
    tableName: TableName,
    processorId: ProcessorID,
    maxProcessingTime: FiniteDuration,
    ttl: FiniteDuration
)

object Config {

  case class TableName(value: String)

}
