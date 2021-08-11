package com.ovoenergy.comms.deduplication
package meteorrepo

import com.ovoenergy.comms.deduplication.model._
import meteor.Client
import meteor.codec.Codec
import meteor.syntax._
import cats.effect._
import cats.implicits._
import java.time.Instant
import meteor.errors
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import scala.concurrent.duration.FiniteDuration
import meteor.CompositeKeysTable
import meteor.Expression
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

object MeteorProcessRepo {

  object fields {
    val id = "id"
    val processorId = "processorId"
    val startedAt = "startedAt"
    val expiresOn = "expiresOn"
    val result = "result"
  }

  implicit def ProcessCodec[ID: Codec, ProcessorID: Codec, A: Codec]
      : Codec[Process[ID, ProcessorID, A]] =
    new Codec[Process[ID, ProcessorID, A]] {
      def read(av: AttributeValue): Either[errors.DecoderError, Process[ID, ProcessorID, A]] =
        (
          av.getAs[ID](fields.id),
          av.getAs[ProcessorID](fields.processorId),
          av.getAs[Instant](fields.startedAt),
          av.getOpt[Instant](fields.expiresOn),
          av.getOpt[A](fields.result)
        ).mapN(Process.apply _)
      def write(process: Process[ID, ProcessorID, A]): AttributeValue =
        Map(
          fields.id -> process.id.asAttributeValue,
          fields.processorId -> process.processorId.asAttributeValue,
          fields.startedAt -> process.startedAt.asAttributeValue,
          fields.expiresOn -> process.expiresOn.asAttributeValue,
          fields.result -> process.result.asAttributeValue
        ).asAttributeValue
    }

  def apply[F[_]: Sync, ID: Codec, ProcessorID: Codec, A: Codec](
      client: Client[F],
      table: CompositeKeysTable[ID, ProcessorID],
      readConsistently: Boolean = false
  ): ProcessRepo[F, ID, ProcessorID, A] = new ProcessRepo[F, ID, ProcessorID, A] {

    def create(
        id: ID,
        processorId: ProcessorID,
        now: Instant
    ): F[Option[Process[ID, ProcessorID, A]]] =
      client
        .update[ID, ProcessorID, Process[ID, ProcessorID, A]](
          table,
          id,
          processorId,
          Expression.apply(
            s"SET #startedAt = if_not_exists(#startedAt, :startedAt)",
            Map("#startedAt" -> fields.startedAt),
            Map(":startedAt" -> now.asAttributeValue)
          ),
          ReturnValue.ALL_OLD
        )

    def markAsCompleted(
        id: ID,
        processorId: ProcessorID,
        result: A,
        now: Instant,
        ttl: Option[FiniteDuration]
    ): F[Unit] =
      client
        .update[ID, ProcessorID, Process[ID, ProcessorID, A]](
          table,
          id,
          processorId,
          Expression.apply(
            s"SET #result = :result, #expiresOn = :expiresOn",
            Map(
              "#result" -> fields.result,
              "#expiresOn" -> fields.expiresOn
            ),
            Map(
              ":result" -> result.asAttributeValue,
              ":expiresOn" -> ttl.map { ttl =>
                Instant.ofEpochMilli(now.toEpochMilli() + ttl.toMillis)
              }.asAttributeValue
            )
          ),
          ReturnValue.ALL_NEW
        )
        .as(())

    def get(
        id: ID,
        processorId: ProcessorID
    ): F[Option[Process[ID, ProcessorID, A]]] =
      client.get[ID, ProcessorID, Process[ID, ProcessorID, A]](
        table,
        id,
        processorId,
        readConsistently
      )

    def attemptReplacing(
        id: ID,
        processorId: ProcessorID,
        oldStartedAt: Instant,
        newStartedAt: Instant
    ): F[ProcessRepo.AttemptResult] =
      client
        .update[ID, ProcessorID, Process[ID, ProcessorID, A]](
          table,
          id,
          processorId,
          Expression.apply(
            s"SET #startedAt = :newStartedAt REMOVE #result",
            Map("#startedAt" -> fields.startedAt, "#result" -> fields.result),
            Map(":newStartedAt" -> newStartedAt.asAttributeValue)
          ),
          Expression.apply(
            s"#startedAt = :oldStartedAt",
            Map("#startedAt" -> fields.startedAt),
            Map(":oldStartedAt" -> oldStartedAt.asAttributeValue)
          ),
          ReturnValue.ALL_OLD
        )
        .as[ProcessRepo.AttemptResult](ProcessRepo.AttemptSucceded)
        .recover {
          case errors.ConditionalCheckFailed(_) => ProcessRepo.AttemptFailed
        }

  }

}
