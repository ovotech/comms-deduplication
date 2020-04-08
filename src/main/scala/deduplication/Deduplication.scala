package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.FiniteDuration
import scala.compat.java8.DurationConverters._
import scala.collection.JavaConverters._

import cats._
import cats.implicits._
import cats.effect._
import cats.effect.implicits._

import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.handlers._

import org.scanamo._

import model._

trait Deduplication[F[_], ID] {

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

  def protect(id: ID): Resource[F, ProcessStatus]
}

object Deduplication {

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
    def read(av: DynamoValue): Either[DynamoReadError, Option[T]] =
      read(av.toAttributeValue)

    override def read(av: AttributeValue): Either[DynamoReadError, Option[T]] =
      Option(av)
        .filter(x => !Boolean.unbox(x.isNULL))
        .map(f.read(_).map(Some(_)))
        .getOrElse(Right(Option.empty[T]))

    def write(t: Option[T]): DynamoValue = t.map(f.write).getOrElse(DynamoValue.nil)
  }

  private implicit val instantDynamoFormat: DynamoFormat[Instant] =
    DynamoFormat.coercedXmap[Instant, Long, IllegalArgumentException](Instant.ofEpochMilli)(
      _.toEpochMilli
    )

  private implicit val expirationDynamoFormat: DynamoFormat[Expiration] =
    DynamoFormat.coercedXmap[Expiration, Long, IllegalArgumentException](x =>
      Expiration(Instant.ofEpochSecond(x))
    )(_.instant.getEpochSecond)

  private implicit def processDynamoFormat[ID: DynamoFormat, ProcessorID: DynamoFormat]
      : DynamoFormat[Process[ID, ProcessorID]] = new DynamoFormat[Process[ID, ProcessorID]] {
    def read(av: DynamoValue): Either[DynamoReadError, Process[ID, ProcessorID]] =
      for {
        obj <- av.asObject
          .toRight(NoPropertyOfType("object", av))
          .leftWiden[DynamoReadError]
        id <- obj.get[ID]("id")
        processorId <- obj.get[ProcessorID]("processorId")
        startedAt <- obj.get[Instant]("startedAt")
        completedAt <- obj.get[Option[Instant]]("completedAt")
        expiresOn <- obj.get[Option[Expiration]]("completedAt")
      } yield Process(id, processorId, startedAt, completedAt, expiresOn)

    def write(process: Process[ID, ProcessorID]): DynamoValue =
      DynamoObject(
        "id" -> DynamoFormat[ID].write(process.id),
        "processorId" -> DynamoFormat[ProcessorID].write(process.processorId),
        "startedAt" -> DynamoFormat[Instant].write(process.startedAt),
        "completedAt" -> DynamoFormat[Option[Instant]].write(process.completedAt),
        "expiresOn" -> DynamoFormat[Option[Expiration]].write(process.expiresOn)
      ).toDynamoValue
  }

  def resource[F[_]: Async: ContextShift: Timer, ID: DynamoFormat, ProcessorID: DynamoFormat](
      config: Config[ProcessorID]
  ): Resource[F, Deduplication[F, ID]] = {
    val dynamoDbR: Resource[F, AmazonDynamoDBAsync] =
      Resource.make(Sync[F].delay(AmazonDynamoDBAsyncClientBuilder.defaultClient()))(c =>
        Sync[F].delay(c.shutdown())
      )

    dynamoDbR.map { client => Deduplication(config, client) }
  }

  def apply[F[_]: Async: ContextShift: Timer, ID: DynamoFormat, ProcessorID: DynamoFormat](
      config: Config[ProcessorID],
      client: AmazonDynamoDBAsync
  ): Deduplication[F, ID] = {

    // TODO Shift back
    def update(request: UpdateItemRequest) =
      Async[F]
        .async[UpdateItemResult] { cb =>
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
        .guarantee(ContextShift[F].shift)

    // Unfurtunately Scanamo does not support update of non existing record
    def startProcessingUpdate(
        id: ID,
        processorId: ProcessorID,
        now: Instant
    ): F[Option[Process[ID, ProcessorID]]] = {

      val result = update(
        new UpdateItemRequest()
          .withTableName(config.tableName.value)
          .withKey(
            Map(
              "id" -> DynamoFormat[ID].write(id).toAttributeValue,
              "processorId" -> DynamoFormat[ProcessorID].write(processorId).toAttributeValue
            ).asJava
          )
          .withUpdateExpression("SET startedAt=:startedAt")
          .withExpressionAttributeValues(
            Map(
              ":startedAt" -> instantDynamoFormat.write(now).toAttributeValue
            ).asJava
          )
          .withReturnValues(ReturnValue.ALL_OLD)
      )

      result.map { res =>
        Option(res.getAttributes)
          .filter(_.size > 0)
          .map(xs => new AttributeValue().withM(xs))
          .traverse { atts =>
            DynamoFormat[Process[ID, ProcessorID]]
              .read(atts)
              .leftMap(e => new Exception(show"Error reading old item: ${e}"))
          }
      }.rethrow
    }

    new Deduplication[F, ID] {

      override def processing(id: ID): F[ProcessStatus] = {
        for {
          now <- Timer[F].clock
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
          now <- Timer[F].clock
            .realTime(TimeUnit.MILLISECONDS)
            .map(Instant.ofEpochMilli)
          _ <- update(
            new UpdateItemRequest()
              .withTableName(config.tableName.value)
              .withKey(
                Map(
                  "id" -> DynamoFormat[ID].write(id).toAttributeValue,
                  "processorId" -> DynamoFormat[ProcessorID]
                    .write(config.processorId)
                    .toAttributeValue
                ).asJava
              )
              .withUpdateExpression("SET completedAt=:completedAt, expiresOn=:expiresOn")
              .withExpressionAttributeValues(
                Map(
                  ":completedAt" -> instantDynamoFormat.write(now).toAttributeValue,
                  ":expiresOn" -> new AttributeValue()
                    .withN(now.plus(config.ttl.toJava).getEpochSecond().toString)
                ).asJava
              )
              .withReturnValues(ReturnValue.NONE)
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
          case ProcessStatus.NotStarted | ProcessStatus.Expired => ifNotProcessed
          case ProcessStatus.Completed => ifProcessed
          case ProcessStatus.Started =>
            Sync[F].raiseError(
              new IllegalStateException(
                "If the status is just started this point should never be reached"
              )
            )
        }
      }

      override def protect(id: ID): Resource[F, ProcessStatus] =
        Resource.make(acquire(id)) {
          case ProcessStatus.NotStarted | ProcessStatus.Expired => processed(id)
          case ProcessStatus.Completed => ().pure[F]
          case ProcessStatus.Started =>
            Sync[F].raiseError(
              new IllegalStateException(
                "If the status is just started this point should never be reached"
              )
            )
        }
    }
  }
}
