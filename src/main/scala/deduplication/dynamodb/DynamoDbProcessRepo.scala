package com.ovoenergy.comms.deduplication
package dynamodb

import java.time.Instant

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.compat.java8.DurationConverters._

import cats.implicits._
import cats.effect._
import cats.effect.implicits._

import software.amazon.awssdk.services.dynamodb.model._
import software.amazon.awssdk.services.dynamodb._

import com.ovoenergy.comms.deduplication.model._

object DynamoDbProcessRepo {

  case class Config(tableName: Config.TableName, modifyClientBuilder: DynamoDbAsyncClientBuilder => DynamoDbAsyncClientBuilder = identity)

  object Config {
    case class TableName(value: String)
  }

  implicit class RichAttributeValue(av: AttributeValue) {
    def get[A: DynamoDbDecoder](key: String): Either[DecoderFailure, A] =
      for {
        xs <- DynamoDbDecoder[Map[String, AttributeValue]].read(av)
        x <- DynamoDbDecoder[A].read(xs.getOrElse(key, AttributeValue.builder().nul(true).build()))
      } yield x
  }

  implicit val dynamoDbDecoderForExpiration: DynamoDbDecoder[Expiration] =
    DynamoDbDecoder[Long].flatMap { seconds =>
      DynamoDbDecoder
        .instance[Instant] { _ =>
          Either
            .catchNonFatal(Instant.ofEpochSecond(seconds))
            .leftMap(e => DecoderFailure(e.getMessage(), e.some))
        }
        .map(Expiration(_))
    }

  private object field {
    val id = "id"
    val processorId = "processorId"
    val startedAt = "startedAt"
    val completedAt = "completedAt"
    val expiresOn = "expiresOn"
  }

  def resource[F[_]: Async: ContextShift: Timer, ID: DynamoDbDecoder: DynamoDbEncoder, ProcessorID: DynamoDbDecoder: DynamoDbEncoder](
    config: Config,
  ): Resource[F, ProcessRepo[F, ID, ProcessorID]] = {
    Resource.make(Sync[F].delay(config.modifyClientBuilder(DynamoDbAsyncClient.builder()).build())){client => 
      Sync[F].delay(client.close())
    }.map(client => apply(config, client))
  }

  def apply[F[_]: Async: ContextShift: Timer, ID: DynamoDbDecoder: DynamoDbEncoder, ProcessorID: DynamoDbDecoder: DynamoDbEncoder](
      config: Config,
      client: DynamoDbAsyncClient
  ): ProcessRepo[F, ID, ProcessorID] = {

    def update(request: UpdateItemRequest) = {
      Async[F]
        .async[UpdateItemResponse] { cb =>
          client.updateItem(request).handle { (ok, error) =>
            cb(Option(error).toLeft(ok))
          }

          ()
        }
        .guarantee(ContextShift[F].shift)
    }

    def readProcess(
        attributes: AttributeValue
    ): Either[DecoderFailure, Process[ID, ProcessorID]] =
      for {
        id <- attributes.get[ID](field.id)
        processorId <- attributes.get[ProcessorID](field.processorId)
        startedAt <- attributes.get[Instant](field.startedAt)
        completedAt <- attributes.get[Option[Instant]](field.completedAt)
        expiresOn <- attributes.get[Option[Expiration]](field.expiresOn)
      } yield Process(
        id,
        processorId,
        startedAt,
        completedAt,
        expiresOn
      )

    new ProcessRepo[F, ID, ProcessorID] {

      def startProcessingUpdate(
          id: ID,
          processorId: ProcessorID,
          now: Instant
      ): F[Option[Process[ID, ProcessorID]]] = {

        val startedAtVar = ":startedAt"

        val request = UpdateItemRequest
          .builder()
          .tableName(config.tableName.value)
          .key(
            Map(
              field.id -> DynamoDbEncoder[ID].write(id),
              field.processorId -> DynamoDbEncoder[ProcessorID].write(processorId)
            ).asJava
          )
          .updateExpression(
            s"SET ${field.startedAt} = if_not_exists(${field.startedAt}, ${startedAtVar})"
          )
          .expressionAttributeValues(
            Map(
              startedAtVar -> AttributeValue.builder().n(now.toEpochMilli().toString).build()
            ).asJava
          )
          .returnValues(ReturnValue.ALL_OLD)
          .build()

        update(request).map { res =>
          Option(res.attributes())
            .filter(_.size > 0)
            .map(xs => AttributeValue.builder().m(xs).build())
            .traverse(readProcess)
        }.rethrow
      }

      def completeProcess(
          id: ID,
          processorId: ProcessorID,
          now: Instant,
          ttl: FiniteDuration,
      ): F[Unit] = {
        val completedAtVar = ":completedAt"
        val expiresOnVar = ":expiresOn"

        val request = UpdateItemRequest
          .builder()
          .tableName(config.tableName.value)
          .key(
            Map(
              field.id -> DynamoDbEncoder[ID].write(id),
              field.processorId -> DynamoDbEncoder[ProcessorID].write(processorId)
            ).asJava
          )
          .updateExpression(
            s"SET ${field.completedAt}=${completedAtVar}, ${field.expiresOn}=${expiresOnVar}"
          )
          .expressionAttributeValues(
            Map(
              completedAtVar -> AttributeValue
                .builder()
                .n(now.toEpochMilli().toString)
                .build(),
              expiresOnVar -> AttributeValue
                .builder()
                .n(now.plus(ttl.toJava).getEpochSecond().toString)
                .build()
            ).asJava
          )
          .returnValues(ReturnValue.NONE)
          .build()

        update(request).void
      }

    }

  }

}
