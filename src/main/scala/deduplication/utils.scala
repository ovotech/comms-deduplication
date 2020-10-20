package com.ovoenergy.comms.deduplication

import java.util.concurrent.CompletableFuture

import cats.effect.Concurrent
import cats.implicits._

package object utils {

  def fromCompletableFuture[F[_]: Concurrent, A](f: () => CompletableFuture[A]): F[A] =
    Concurrent[F].cancelable[A] { cb =>

      val future = f()
      future.whenComplete { (ok, err) =>
        cb(Option(err).toLeft(ok))
      }

      Concurrent[F].delay(future.cancel(true)).void
    }

}
