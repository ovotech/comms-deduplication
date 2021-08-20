package com.ovoenergy.comms.deduplication
package meteor

import _root_.meteor.codec.Codec
import _root_.meteor.CompositeKeysTable
import _root_.meteor.Client
import cats.effect._
import com.ovoenergy.comms.deduplication.meteor.model._

object MeteorDeduplication {

  def apply[F[_]: Sync: Temporal, ID: Codec, ContextID: Codec](
      client: Client[F],
      table: CompositeKeysTable[ID, ContextID],
      config: Config
  ): F[Deduplication[F, ID, ContextID, EncodedResult]] =
    Deduplication(
      MeteorProcessRepo(client, table, false),
      config
    )

  def resource[F[_]: Sync: Timer, ID: Codec, ContextID: Codec](
      client: Client[F],
      table: CompositeKeysTable[ID, ContextID],
      config: Config
  ): Resource[F, Deduplication[F, ID, ContextID, EncodedResult]] =
    Resource.eval[F, Deduplication[F, ID, ContextID, EncodedResult]](apply(client, table, config))

}
