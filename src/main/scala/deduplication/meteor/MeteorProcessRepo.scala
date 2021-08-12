package com.ovoenergy.comms.deduplication.meteor

import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.meteor.codecs._
import com.ovoenergy.comms.deduplication.model._
import com.ovoenergy.comms.deduplication.{meteor => _, _}
import java.time.Instant
import meteor.Client
import meteor.CompositeKeysTable
import meteor.Expression
import meteor.codec.Codec
import meteor.errors
import meteor.syntax._
import scala.concurrent.duration.FiniteDuration
import software.amazon.awssdk.services.dynamodb.model.ReturnValue

object MeteorProcessRepo {

  def apply[F[_]: Sync, ID: Codec, ContextID: Codec, A: Codec](
      client: Client[F],
      table: CompositeKeysTable[ID, ContextID],
      readConsistently: Boolean = false
  ): ProcessRepo[F, ID, ContextID, A] = new ProcessRepo[F, ID, ContextID, A] {

    def create(
        id: ID,
        contextId: ContextID,
        now: Instant
    ): F[Option[Process[ID, ContextID, A]]] =
      client
        .update[ID, ContextID, Process[ID, ContextID, A]](
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
        result: A,
        now: Instant,
        ttl: Option[FiniteDuration]
    ): F[Unit] =
      client
        .update[ID, ContextID, Process[ID, ContextID, A]](
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
    ): F[Option[Process[ID, ContextID, A]]] =
      client.get[ID, ContextID, Process[ID, ContextID, A]](
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
        .update[ID, ContextID, Process[ID, ContextID, A]](
          table,
          id,
          contextId,
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
