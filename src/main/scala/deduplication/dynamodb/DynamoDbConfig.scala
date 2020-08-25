package com.ovoenergy.comms.deduplication
package dynamodb

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder

case class DynamoDbConfig(
    tableName: DynamoDbConfig.TableName,
    modifyClientBuilder: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity
)

object DynamoDbConfig {
  case class TableName(value: String)
}
