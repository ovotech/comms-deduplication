package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.compat.java8.DurationConverters._
import scala.collection.JavaConverters._

import cats._
import cats.effect._
import cats.implicits._

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.handlers._

import com.gu.scanamo._
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.syntax._
import com.gu.scanamo.error._

import model._

trait ProcessingStore[F[_], ID] {

  /**
    * Try to acquire a lock on a process
    *
    * Returns a [[ProcessStatus]] instance:
    *  - [[ProcessStatus.NotStarted]] if the process has not been started yet
    *  - [[ProcessStatus.Started]] if the process has been started but not completed
    *  - [[ProcessStatus.Completed]] if the process has been completed
    *  - [[ProcessStatus.Expired]] if the process has started but has not been completed in time
    */
  def processing(id: ID): F[ProcessStatus]

  def processed(id: ID): F[Unit]

  def protect[A](id: ID, ifNotProcessed: F[A], ifProcessed: F[A]): F[A]

  def protect[A](id: ID): Resource[F, ProcessStatus]
}

object ProcessingStore {

  def processStatus[F[_]: Monad: Clock](
      maxProcessingTime: FiniteDuration
  )(p: Process[_, _]): F[ProcessStatus] =
    if (p.completedAt.isDefined) {
      Monad[F].point(ProcessStatus.Completed)
    } else {
      Clock[F]
        .realTime(TimeUnit.MILLISECONDS)
        .map(Instant.ofEpochMilli)
        .map { now =>
          /*
           * If the startedAt is:
           *  - In the past compared to expected finishing time the processed has expired
           *  - In the future compared to expected finishing time present the process has started but not yet completed
           */
          val isExpired = p.startedAt
            .plus(maxProcessingTime.toJava)
            .isBefore(now)

          if (isExpired)
            ProcessStatus.Expired
          else
            ProcessStatus.Started
        }
    }

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
      _.toEpochMilli
    )

  private implicit val expirationDynamoFormat: DynamoFormat[Expiration] =
    DynamoFormat.coercedXmap[Expiration, Long, IllegalArgumentException](
      x => Expiration(Instant.ofEpochSecond(x))
    )(_.instant.getEpochSecond)

  def resource[F[_]: Async, ID, ProcessorID](config: Config[ProcessorID])(
      implicit idDf: DynamoFormat[ID],
      processorIdDf: DynamoFormat[ProcessorID],
      timer: Timer[F],
      ec: ExecutionContext
  ): Resource[F, ProcessingStore[F, ID]] = {
    val dynamoDbR: Resource[F, AmazonDynamoDBAsync] =
      Resource.make(Sync[F].delay(AmazonDynamoDBAsyncClientBuilder.defaultClient()))(
        c => Sync[F].delay(c.shutdown())
      )

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

    implicit val processDf = DynamoFormat[Process[ID, ProcessorID]]

    def scanamoF[A]: ScanamoOps[A] => F[A] = new ScanamoF[F].exec[A](client)

    // Unfurtunately Scanamo does not support update of non existing record
    def startProcessingUpdate(
        id: ID,
        processorId: ProcessorID,
        now: Instant
    ): F[Option[Process[ID, ProcessorID]]] = {

      val request = new UpdateItemRequest()
        .withTableName(config.tableName.value)
        .withKey(
          Map("id" -> idDf.write(id), "processorId" -> processorIdDf.write(processorId)).asJava
        )
        .withUpdateExpression("SET startedAt=:startedAt")
        .withExpressionAttributeValues(
          Map(
            ":startedAt" -> instantDynamoFormat.write(now)
          ).asJava
        )
        .withReturnValues(ReturnValue.ALL_OLD)

      val result = Async[F].async[UpdateItemResult] { cb =>
        client.updateItemAsync(
          request,
          new AsyncHandler[UpdateItemRequest, UpdateItemResult] {
            def onError(exception: Exception) = {
              cb(Left(exception))
            }

            def onSuccess(req: UpdateItemRequest, res: UpdateItemResult) = {
              cb(Right(res))
            }
          }
        )

        ()
      }

      result.map { res =>
        Option(res.getAttributes)
          .filter(_.size > 0)
          .map(xs => new AttributeValue().withM(xs))
          .map { atts =>
            processDf
              .read(atts)
              .leftMap(e => new Exception("Error reading old item: ${e.show}"): Throwable)
          }
          .sequence
      }.rethrow
    }

    new ProcessingStore[F, ID] {

      private val table: Table[Process[ID, ProcessorID]] =
        Table[Process[ID, ProcessorID]](config.tableName.value)

      override def processing(id: ID): F[ProcessStatus] = {
        for {
          now <- timer.clock
            .realTime(TimeUnit.MILLISECONDS)
            .map(Instant.ofEpochMilli)
          processOpt <- startProcessingUpdate(id, config.processorId, now)
          status <- processOpt
            .traverse(processStatus[F](config.maxProcessingTime))
            .map(_.getOrElse(ProcessStatus.NotStarted))
        } yield status
      }

      override def processed(id: ID): F[Unit] = {
        for {
          now <- timer.clock
            .realTime(TimeUnit.MILLISECONDS)
            .map(Instant.ofEpochMilli)
          result <- scanamoF(
            table.update(
              ('id -> id and 'processorId -> config.processorId),
              set('completedAt -> Some(now)) and set(
                'expiresOn -> Expiration(now.plus(config.ttl.toJava))
              )
            )
          )
        } yield ()
      }

      private def acquire(id: ID): F[ProcessStatus] = {

        val nowF = Clock[F].monotonic(TimeUnit.MILLISECONDS)
        val pollStrategy = config.pollStrategy

        def doIt(
            startedAt: Long,
            pollNo: Int,
            pollDelay: FiniteDuration
        ): F[ProcessStatus] = {
          processing(id).flatMap {
            case ProcessStatus.Started =>
              val totalDurationF =
                nowF.map(_ - startedAt).map(FiniteDuration(_, TimeUnit.MILLISECONDS))

              // retry until it is either Completed or Expired
              totalDurationF
                .map(_ >= pollStrategy.maxPollDuration)
                .ifM(
                  Sync[F].raiseError(new TimeoutException(s"Stop polling after ${pollNo} polls")),
                  Timer[F].sleep(pollDelay) >> doIt(
                    startedAt,
                    pollNo + 1,
                    config.pollStrategy.nextDelay(pollNo, pollDelay)
                  )
                )
            case status =>
              Sync[F].point(status)
          }
        }

        nowF.flatMap(now => doIt(now, 0, pollStrategy.initialDelay))
      }

      override def protect[A](id: ID, ifNotProcessed: F[A], ifProcessed: F[A]): F[A] = {
        protect(id).use {
          case ProcessStatus.NotStarted => ifNotProcessed
          case ProcessStatus.Expired | ProcessStatus.Completed => ifProcessed
          case ProcessStatus.Started =>
            Sync[F].raiseError(
              new IllegalStateException(
                "If the status is just started this point should never be reached"
              )
            )
        }
      }

      override def protect[A](id: ID): Resource[F, ProcessStatus] =
        Resource.make(acquire(id))(_ => processed(id))
    }
  }
}
