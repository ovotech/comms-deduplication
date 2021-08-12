package com.ovoenergy.comms.deduplication

import cats.effect._
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

trait Deduplication[F[_], ID, ContextID, A] {
  def context(id: ContextID): DeduplicationContext[F, ID, ContextID, A]
}

object Deduplication {
  def apply[F[_]: Sync: Timer, ID, ContextID, A](
      processRepo: ProcessRepo[F, ID, ContextID, A],
      config: Config
  ): F[Deduplication[F, ID, ContextID, A]] = Slf4jLogger.create[F].map { logger =>
    new Deduplication[F, ID, ContextID, A] {
      def context(id: ContextID): DeduplicationContext[F, ID, ContextID, A] =
        DeduplicationContext[F, ID, ContextID, A](id, processRepo, config, logger)
    }

  }
}
