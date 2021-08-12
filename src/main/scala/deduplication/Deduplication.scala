package com.ovoenergy.comms.deduplication

import cats.effect._
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

trait Deduplication[F[_], ID, ContextID, Encoded] {
  def context[A](id: ContextID)(
      implicit codec: ResultCodec[Encoded, A]
  ): DeduplicationContext[F, ID, ContextID, A]
}

object Deduplication {
  def apply[F[_]: Sync: Timer, ID, ContextID, Encoded](
      processRepo: ProcessRepo[F, ID, ContextID, Encoded],
      config: Config
  ): F[Deduplication[F, ID, ContextID, Encoded]] = Slf4jLogger.create[F].map { logger =>
    new Deduplication[F, ID, ContextID, Encoded] {
      def context[A](
          id: ContextID
      )(implicit codec: ResultCodec[Encoded, A]): DeduplicationContext[F, ID, ContextID, A] =
        DeduplicationContext[F, ID, ContextID, Encoded, A](id, processRepo, config, logger)
    }

  }
}
