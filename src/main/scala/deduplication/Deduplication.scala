package com.ovoenergy.comms.deduplication

import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import scala.concurrent.duration._
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
import org.scanamo.error.{DynamoReadError, NoPropertyOfType}

import model._
import ScanamoHelpers._

/**
  *
  */
trait Deduplication[F[_], ID] {

  /**
    * Try to start a process.
    *
    * If the process with the giving id has never started before, this will start a new process and return Some(complete)
    * If another process with the same id has already completed, this will do nothing and return None
    * If another process with the same id has already started and timeouted, this will do nothing and return None
    * If another process with the same id is still running, this will wait until it will complete or timeout
    *
    * If markAsComplete fails, the process will likely be duplicated.
    * If the process takes more time than maxProcessingTime, you may have duplicate if two processes with same ID happen at the same time
    *
    * eg
    * ```
    * tryStartProcess(id)
    *   .flatMap {
    *     case Outcome.New(markAsComplete) =>
    *       doYourStuff.flatTap(_ => markAsComplete)
    *     case Outcome.Duplicate() =>
    *       dontDoYourStuff
    *   }
    * ```
    *
    * @param id The process id to start
    * @return An Outcome.New or Outcome.Duplicate. The Outcome.New will contain an effect to complete the just started process.
    */
  def tryStartProcess(id: ID): F[Outcome[F]]

  /**
    * Do the best effort to ensure a process to be successfully executed only once.
    *
    * If the process has already runned successfully before, it will run the [[ifDuplicate]].
    * Otherwise, it will run the [[ifNew]].
    *
    * The return value is either the result of [[ifNew]] or [[ifDuplicate]].
    *
    * @param id The id of the process to run.
    * @param ifNew The effect to run if the process is new.
    * @param ifDuplicate The effect to run if the process is duplicate.
    * @return the result of [[ifNew]] or [[ifDuplicate]].
    */
  def protect[A](id: ID, ifNew: F[A], ifDuplicate: F[A]): F[A]
}

object Deduplication {

  private object field {
    val id = "id"
    val processorId = "processorId"
    val startedAt = "startedAt"
    val completedAt = "completedAt"
    val expiresOn = "expiresOn"
  }

  def nowF[F[_]: Functor: Clock] =
    Clock[F]
      .realTime(TimeUnit.MILLISECONDS)
      .map(Instant.ofEpochMilli)

  def processStatus[F[_]: Monad: Clock](
      maxProcessingTime: FiniteDuration
  )(p: Process[_, _]): F[ProcessStatus] =
    if (p.completedAt.isDefined) {
      ProcessStatus.Completed.pure[F].widen
    } else {
      nowF[F]
        .map { now =>
          /*
           * If the startedAt is:
           *  - In the past compared to expected finishing time the processed has timeout
           *  - In the future compared to expected finishing time present the process has started but not yet completed
           */
          val isTimeout = p.startedAt
            .plus(maxProcessingTime.toJava)
            .isBefore(now)

          if (isTimeout)
            ProcessStatus.Timeout
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
        id <- obj.get[ID](field.id)
        processorId <- obj.get[ProcessorID](field.processorId)
        startedAt <- obj.get[Instant](field.startedAt)
        completedAt <- obj.get[Option[Instant]](field.completedAt)
        expiresOn <- obj.get[Option[Expiration]](field.expiresOn)
      } yield Process(id, processorId, startedAt, completedAt, expiresOn)

    def write(process: Process[ID, ProcessorID]): DynamoValue =
      DynamoObject(
        field.id -> DynamoFormat[ID].write(process.id),
        field.processorId -> DynamoFormat[ProcessorID].write(process.processorId),
        field.startedAt -> DynamoFormat[Instant].write(process.startedAt),
        field.completedAt -> DynamoFormat[Option[Instant]].write(process.completedAt),
        field.expiresOn -> DynamoFormat[Option[Expiration]].write(process.expiresOn)
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

      val startedAtVar = ":startedAt"

      val result = update(
        new UpdateItemRequest()
          .withTableName(config.tableName.value)
          .withKey(
            Map(
              field.id -> DynamoFormat[ID].write(id).toAttributeValue,
              field.processorId -> DynamoFormat[ProcessorID].write(processorId).toAttributeValue
            ).asJava
          )
          .withUpdateExpression(
            s"SET ${field.startedAt} = if_not_exists(${field.startedAt}, ${startedAtVar})"
          )
          .withExpressionAttributeValues(
            Map(
              startedAtVar -> instantDynamoFormat.write(now).toAttributeValue
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

      override def tryStartProcess(id: ID): F[Outcome[F]] = {

        val completeProcess: F[Unit] = {

          val completedAtVar = ":completedAt"
          val expiresOnVar = ":expiresOn"

          for {
            now <- nowF[F]
            _ <- update(
              new UpdateItemRequest()
                .withTableName(config.tableName.value)
                .withKey(
                  Map(
                    field.id -> DynamoFormat[ID].write(id).toAttributeValue,
                    field.processorId -> DynamoFormat[ProcessorID]
                      .write(config.processorId)
                      .toAttributeValue
                  ).asJava
                )
                .withUpdateExpression(
                  s"SET ${field.completedAt}=${completedAtVar}, ${field.expiresOn}=${expiresOnVar}"
                )
                .withExpressionAttributeValues(
                  Map(
                    completedAtVar -> instantDynamoFormat.write(now).toAttributeValue,
                    expiresOnVar -> new AttributeValue()
                      .withN(now.plus(config.ttl.toJava).getEpochSecond().toString)
                  ).asJava
                )
                .withReturnValues(ReturnValue.NONE)
            )
          } yield ()
        }

        val pollStrategy = config.pollStrategy

        def doIt(
            startedAt: Instant,
            pollNo: Int,
            pollDelay: FiniteDuration
        ): F[Outcome[F]] = {

          def nextStep(ps: ProcessStatus): F[Outcome[F]] = ps match {
            case ProcessStatus.Started =>
              val totalDurationF = nowF[F]
                .map(now => (now.toEpochMilli - startedAt.toEpochMilli).milliseconds)

              // retry until it is either Completed or Timeout
              totalDurationF
                .map(td => td >= pollStrategy.maxPollDuration)
                .ifM(
                  Sync[F].raiseError(new TimeoutException(s"Stop polling after ${pollNo} polls")),
                  Timer[F].sleep(pollDelay) >> doIt(
                    startedAt,
                    pollNo + 1,
                    config.pollStrategy.nextDelay(pollNo, pollDelay)
                  )
                )
            case ProcessStatus.NotStarted | ProcessStatus.Timeout =>
              Outcome.New(completeProcess).pure[F].widen[Outcome[F]]

            case ProcessStatus.Completed =>
              Monad[F].point(Outcome.Duplicate())
          }

          for {
            now <- nowF[F]
            processOpt <- startProcessingUpdate(id, config.processorId, now)
            status <- processOpt
              .traverse(processStatus[F](config.maxProcessingTime))
              .map(_.getOrElse(ProcessStatus.NotStarted))
            sample <- nextStep(status)
          } yield sample
        }

        nowF[F].flatMap(now => doIt(now, 0, pollStrategy.initialDelay))
      }

      override def protect[A](id: ID, ifNotSeen: F[A], ifSeen: F[A]): F[A] = {
        tryStartProcess(id)
          .flatMap {
            case Outcome.New(markAsComplete) =>
              ifNotSeen.flatTap(_ => markAsComplete)
            case Outcome.Duplicate() =>
              ifSeen
          }
      }
    }
  }
}

/*
 * TODO: Remove this once upgrade to later version of Scanamo
 * This method exists in 1.0.0-M12 but because of this issue:
 * https://github.com/scanamo/scanamo/issues/583, we cannot use M12 because
 * it writes List[T] to a Map if T is not a primitive type. As a result, we
 * need to use M11 which does not have this `get` method
 */
object ScanamoHelpers {
  implicit class DynObjExtras(obj: DynamoObject) {
    def get[A: DynamoFormat](key: String): Either[DynamoReadError, A] =
      obj(key).getOrElse(DynamoValue.nil).as[A]
  }
}
