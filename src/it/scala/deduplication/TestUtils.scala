package com.ovoenergy.comms.deduplication

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.concurrent.duration._

import java.util.UUID

import cats._
import cats.effect._
import cats.implicits._

import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb.{model => _, _}

import com.ovoenergy.comms.deduplication.dynamodb._
import com.ovoenergy.comms.deduplication.utils._

package object TestUtils {

  def processRepoResource[F[_]: Concurrent: ContextShift: Timer]
      : Resource[F, ProcessRepo[F, UUID, UUID]] =
    for {
      dynamoclient <- dynamoClientResource[F]
      tableName <- Resource.liftF(randomTableName)
      table <- tableResource[F](dynamoclient, tableName)
    } yield DynamoDbProcessRepo[F, UUID, UUID](
      DynamoDbConfig(DynamoDbConfig.TableName(table)),
      dynamoclient
    )

  def tableResource[F[_]: Concurrent: Timer](
      client: DynamoDbAsyncClient,
      tableName: String
  ): Resource[F, String] =
    Resource.make(createTable(client, tableName))(deleteTable(client, _))

  def createTable[F[_]: Concurrent: Timer](
      client: DynamoDbAsyncClient,
      tableName: String
  ): F[String] = {
    val createTableRequest =
      CreateTableRequest
        .builder()
        .tableName(tableName)
        .billingMode(BillingMode.PAY_PER_REQUEST)
        .keySchema(
          List(
            dynamoKey(DynamoDbProcessRepo.field.id, KeyType.HASH),
            dynamoKey(DynamoDbProcessRepo.field.processorId, KeyType.RANGE)
          ).asJava
        )
        .attributeDefinitions(
          dynamoAttribute(DynamoDbProcessRepo.field.id, ScalarAttributeType.S),
          dynamoAttribute(DynamoDbProcessRepo.field.processorId, ScalarAttributeType.S)
        )
        .build()
    fromCompletableFuture[F, CreateTableResponse](() => client.createTable(createTableRequest))
      .map(_.tableDescription().tableName())
      .flatTap(waitForTableCreation(client, _))
      .onError {
        case NonFatal(e) => Concurrent[F].delay(println(s"Error creating DynamoDb table: $e"))
      }
  }

  def deleteTable[F[_]: Concurrent](client: DynamoDbAsyncClient, tableName: String): F[Unit] = {
    val deleteTableRequest = DeleteTableRequest.builder().tableName(tableName).build()
    fromCompletableFuture[F, DeleteTableResponse](() => client.deleteTable(deleteTableRequest)).void
      .onError {
        case NonFatal(e) => Concurrent[F].delay(println(s"Error creating DynamoDb table: $e"))
      }
  }

  def waitForTableCreation[F[_]: Concurrent: Timer](
      client: DynamoDbAsyncClient,
      tableName: String,
      pollEveryMs: FiniteDuration = 100.milliseconds
  )(implicit ME: MonadError[F, Throwable]): F[Unit] = {
    val request = DescribeTableRequest.builder().tableName(tableName).build()
    for {
      response <- fromCompletableFuture(() => client.describeTable(request))
      _ <- response.table().tableStatus() match {
        case TableStatus.ACTIVE => ().pure[F]
        case TableStatus.CREATING | TableStatus.UPDATING =>
          Timer[F].sleep(pollEveryMs) >> waitForTableCreation(client, tableName, pollEveryMs)
        case status =>
          ME.raiseError(
            new Exception(s"Unexpected status ${status.toString()} for table ${tableName}")
          )
      }
    } yield ()
  }

  def dynamoKey(attributeName: String, keyType: KeyType): KeySchemaElement =
    KeySchemaElement
      .builder()
      .attributeName(attributeName)
      .keyType(keyType)
      .build()

  def dynamoAttribute(
      attributeName: String,
      attributeType: ScalarAttributeType
  ): AttributeDefinition =
    AttributeDefinition
      .builder()
      .attributeName(attributeName)
      .attributeType(attributeType)
      .build()

  def dynamoClientResource[F[_]: Sync]: Resource[F, DynamoDbAsyncClient] =
    Resource.make(Sync[F].delay(DynamoDbAsyncClient.builder.build()))(c => Sync[F].delay(c.close()))

  def randomTableName[F[_]: Sync]: F[String] =
    Sync[F].delay(s"comms-deduplication-test-${UUID.randomUUID()}")

}