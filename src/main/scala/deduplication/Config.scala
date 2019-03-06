package deduplication

import java.time.Duration
import deduplication.Config.TableName

case class Config[ProcessorID](tableName: TableName, processorId: ProcessorID, ttl: Duration)

object Config {

  case class TableName(value: String)

}
