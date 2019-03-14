package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext

import cats.effect._
import cats.implicits._

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder}

import com.gu.scanamo._
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.syntax._

import model._

trait ProcessingStore[F[_], ID] {

  def processing(id: ID): F[Boolean]

  def processed(id: ID): F[Unit]

  def protect[A](id: ID, ifNotProcessed: F[A], ifProcessed: F[A]): F[A]
}

object ProcessingStore {

  private implicit val statusDynamoFormat: DynamoFormat[Status] =
    DynamoFormat.coercedXmap[Status, String, IllegalArgumentException](Status.unsafeFromString)(
      _.toString)

  private implicit val expirationDynamoFormat: DynamoFormat[Expiration] =
    DynamoFormat.coercedXmap[Expiration, Long, IllegalArgumentException](x =>
      Expiration(Instant.ofEpochSecond(x)))(_.instant.getEpochSecond)

  def resource[F[_]: Async, ID, ProcessorID](config: Config[ProcessorID])(
      implicit idDf: DynamoFormat[ID],
      processorIdDf: DynamoFormat[ProcessorID],
      timer: Timer[F],
      ec: ExecutionContext
  ): Resource[F, ProcessingStore[F, ID]] = {
    val dynamoDbR: Resource[F, AmazonDynamoDBAsync] =
      Resource.make(Sync[F].delay(AmazonDynamoDBAsyncClientBuilder.defaultClient()))(c =>
        Sync[F].delay(c.shutdown()))

    dynamoDbR.map { client =>
      ProcessingStore(config, client)
    }
  }

  def apply[F[_]: Async, ID, ProcessorID](config: Config[ProcessorID], client: AmazonDynamoDBAsync)(
      implicit idDf: DynamoFormat[ID],
      processorIdDf: DynamoFormat[ProcessorID],
      timer: Timer[F],
      ec: ExecutionContext
  ): ProcessingStore[F, ID] = {

    def scanamoF[A]: ScanamoOps[A] => F[A] = new ScanamoF[F].exec[A](client)

    new ProcessingStore[F, ID] {

      private val table: Table[Process[ID, ProcessorID]] =
        Table[Process[ID, ProcessorID]](config.tableName.value)

      override def processing(id: ID): F[Boolean] = {
        for {
          now <- timer.clock
            .realTime(TimeUnit.SECONDS)
            .map(Instant.ofEpochSecond)
            .map(Expiration.apply)
          result <- scanamoF(
            table
              .given(
                (not('id -> id) and not('processId -> config.processorId)) or ('expiredOn >= now and 'status -> Status.processing))
              .put(Process(id, config.processorId, Status.processing, Some(now.plus(config.ttl)))))
            .map(_.fold(_ => false, _ => true))
        } yield result
      }

      override def processed(id: ID): F[Unit] = {
        scanamoF(table.put(Process(id, config.processorId, Status.processed, None))).void
      }

      override def protect[A](id: ID, ifNotProcessed: F[A], ifProcessed: F[A]): F[A] = {
        processing(id).ifM(ifNotProcessed <* processed(id), ifProcessed)
      }
    }
  }
}
