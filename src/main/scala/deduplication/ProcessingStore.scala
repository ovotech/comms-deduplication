package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext
import scala.compat.java8.DurationConverters._

import cats.effect._
import cats.implicits._

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder}
import com.amazonaws.services.dynamodbv2.model.AttributeValue

import com.gu.scanamo._
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.syntax._
import com.gu.scanamo.error._

import model._

trait ProcessingStore[F[_], ID] {

  def processing(id: ID): F[Boolean]

  def processed(id: ID): F[Unit]

  def protect[A](id: ID, ifNotProcessed: F[A], ifProcessed: F[A]): F[A]
}

object ProcessingStore {

  private implicit def optionFormat[T](implicit f: DynamoFormat[T]) = new DynamoFormat[Option[T]] {
    def read(av: AttributeValue): Either[DynamoReadError, Option[T]] =
      Option(av)
        .filter(x => !Boolean.unbox(x.isNULL))
        .map(f.read(_).map(Some(_)))
        .getOrElse(Right(Option.empty[T]))

    def write(t: Option[T]): AttributeValue =
      t.map(f.write).getOrElse(new AttributeValue().withNULL(true))
    override val default = Some(None)
  }

  private implicit val instantDynamoFormat: DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, Long, IllegalArgumentException](Instant.ofEpochMilli)(
      _.toEpochMilli)

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
            .realTime(TimeUnit.MILLISECONDS)
            .map(Instant.ofEpochMilli)

          result <- scanamoF(
            table.put(
              Process(
                id,
                config.processorId,
                now,
                None,
                Some(Expiration(now.plus(config.ttl.toJava)))))
          )

          processOpt <- result.sequence
            .leftMap(e => new Exception("Error reading old item: ${e.show}"))
            .fold(_.raiseError[F, Option[Process[ID, ProcessorID]]], _.pure[F])

        } yield {
          // Returns true if:
          // - the process does not exist
          // - the process exist but it is completed
          // - the process exist, is not completed but it is expired
          processOpt.fold(true) { process =>
            process.completedAt.isEmpty && process.expiresOn.fold(true)(_.instant.isBefore(now))
          }
        }
      }

      override def processed(id: ID): F[Unit] = {
        for {
          now <- timer.clock
            .realTime(TimeUnit.MILLISECONDS)
            .map(Instant.ofEpochMilli)
          result <- scanamoF(
            table.update(
              ('id -> id and 'processorId -> config.processorId),
              set('completedAt -> Some(now)) and set('expiresOn -> none[Expiration])))
        } yield ()
      }

      override def protect[A](id: ID, ifNotProcessed: F[A], ifProcessed: F[A]): F[A] = {
        processing(id).ifM(ifNotProcessed <* processed(id), ifProcessed)
      }
    }
  }
}
