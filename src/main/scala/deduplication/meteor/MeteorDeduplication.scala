package com.ovoenergy.comms.deduplication.meteor

import com.ovoenergy.comms.deduplication.{meteor => _, _}
import meteor.codec.Codec
import meteor.CompositeKeysTable
import meteor.Client
import cats.effect._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object MeteorDeduplication {

  def apply[F[_]: Sync: Timer, ID: Codec, ContextID: Codec](
      client: Client[F],
      table: CompositeKeysTable[ID, ContextID],
      config: Config
  ): F[Deduplication[F, ID, ContextID, AttributeValue]] =
    Deduplication(
      MeteorProcessRepo(client, table, false),
      config
    )

  def resource[F[_]: Sync: Timer, ID: Codec, ContextID: Codec, A: Codec](
      client: Client[F],
      table: CompositeKeysTable[ID, ContextID],
      config: Config
  ): Resource[F, Deduplication[F, ID, ContextID, AttributeValue]] =
    Resource.eval[F, Deduplication[F, ID, ContextID, AttributeValue]](apply(client, table, config))

}
