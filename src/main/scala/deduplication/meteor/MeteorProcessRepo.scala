package com.ovoenergy.comms.deduplication
package meteor

import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.meteor.codecs._
import com.ovoenergy.comms.deduplication.meteor.model._
import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import _root_.meteor.Client
import _root_.meteor.CompositeKeysTable
import _root_.meteor.Expression
import _root_.meteor.codec.Codec
import _root_.meteor.errors
import _root_.meteor.syntax._
import scala.concurrent.duration.FiniteDuration
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

object MeteorProcessRepo {

  def apply[F[_]: Sync, ID: Codec, ContextID: Codec](
      client: Client[F],
      table: CompositeKeysTable[ID, ContextID],
      readConsistently: Boolean = false
  ): ProcessRepo[F, ID, ContextID, EncodedResult] =
    new ProcessRepo[F, ID, ContextID, EncodedResult] {

      def create(
          id: ID,
          contextId: ContextID,
          now: Instant
      ): F[Option[Process[ID, ContextID, EncodedResult]]] =
        client
          .update[ID, ContextID, Process[ID, ContextID, EncodedResult]](
            table,
            id,
            contextId,
            Expression.apply(
              s"SET #startedAt = if_not_exists(#startedAt, :startedAt)",
              Map("#startedAt" -> fields.startedAt),
              Map(":startedAt" -> now.asAttributeValue)
            ),
            ReturnValue.ALL_OLD
          )

      def markAsCompleted(
          id: ID,
          contextId: ContextID,
          result: EncodedResult,
          now: Instant,
          ttl: Option[FiniteDuration]
      ): F[Unit] =
        client
          .update[ID, ContextID, Process[ID, ContextID, EncodedResult]](
            table,
            id,
            contextId,
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
          contextId: ContextID
      ): F[Option[Process[ID, ContextID, EncodedResult]]] =
        client.get[ID, ContextID, Process[ID, ContextID, EncodedResult]](
          table,
          id,
          contextId,
          readConsistently
        )

      def attemptReplacing(
          id: ID,
          contextId: ContextID,
          oldStartedAt: Instant,
          newStartedAt: Instant
      ): F[ProcessRepo.AttemptResult] =
        client
          .update[ID, ContextID, Process[ID, ContextID, EncodedResult]](
            table,
            id,
            contextId,
            Expression.apply(
              s"SET #startedAt = :newStartedAt REMOVE #result, #expiresOn",
              Map(
                "#startedAt" -> fields.startedAt,
                "#result" -> fields.result,
                "#expiresOn" -> fields.expiresOn
              ),
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
