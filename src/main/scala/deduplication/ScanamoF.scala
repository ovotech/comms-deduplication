package com.ovoenergy.comms.deduplication

import scala.concurrent.ExecutionContext

import cats.effect.Async

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync

import com.gu.scanamo._
import com.gu.scanamo.ops.ScanamoOps

class ScanamoF[F[_]] {

  def exec[A](
      dynamoDb: AmazonDynamoDBAsync
  )(ops: ScanamoOps[A])(implicit ec: ExecutionContext, F: Async[F]): F[A] = {
    F.async[A] { cb =>
      ScanamoAsync.exec[A](dynamoDb)(ops).onComplete(result => cb(result.toEither))
    }
  }
}
