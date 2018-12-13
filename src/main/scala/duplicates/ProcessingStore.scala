package duplicates

import java.time.{Instant, Duration}
import java.util.concurrent.TimeUnit

import cats.effect._
import cats.implicits._
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsyncClientBuilder, AmazonDynamoDBAsync}
import com.gu.scanamo._
import com.gu.scanamo.ops.ScanamoOps
import com.gu.scanamo.syntax._

import scala.concurrent.ExecutionContext


trait ProcessingStore[F[_], ID] {

  def processing(id: ID): F[Boolean]

  def processed(id: ID): F[Unit]

  def protect[A](id: ID, ifNotProcessed: F[A], ifProcessed: F[A]): F[A]
}


object ProcessingStore {

  sealed trait Status

  object Status {

    case object Processing extends Status

    case object Processed extends Status

    val processing: Status = Processing
    val processed: Status = Processed

    def fromString(s: String): Option[Status] = s match {
      case "Processing" => Processing.some
      case "Processed" => Processed.some
      case _ => none[Status]
    }

    def unsafeFromString(s: String): Status = fromString(s)
      .getOrElse(throw new IllegalArgumentException(s"$s is not a valid Status"))
  }

  case class Expiration(instant: Instant) {

    def plus(d: Duration): Expiration =
      copy(instant.plus(d))
  }

  private implicit val statusDynamoFormat: DynamoFormat[Status] = DynamoFormat.coercedXmap[Status, String, IllegalArgumentException](Status.unsafeFromString)(_.toString)

  private implicit val expirationDynamoFormat: DynamoFormat[Expiration] = DynamoFormat.coercedXmap[Expiration, Long, IllegalArgumentException](x => Expiration(Instant.ofEpochSecond(x)))(_.instant.getEpochSecond)


  def fromDynamo[F[_] : Async, ID, ProcessorID](config: DynamoProcessingStoreConfig[ProcessorID])(
    implicit idDf: DynamoFormat[ID],
    processorIdDf: DynamoFormat[ProcessorID],
    timer: Timer[F],
    ec: ExecutionContext
  ): Resource[F, ProcessingStore[F, ID]] = {

    val dynamoDbR: Resource[F, AmazonDynamoDBAsync] = Resource.make(Sync[F].delay(AmazonDynamoDBAsyncClientBuilder.defaultClient()))(c => Sync[F].delay(c.shutdown()))

    dynamoDbR.map { dynamoDb =>

      def scanamoF[A]: ScanamoOps[A] => F[A] = new ScanamoF[F].exec[A](dynamoDb)

      case class Process(id: ID, processorId: ProcessorID, status: Status, expiredOn: Option[Expiration])

      new ProcessingStore[F, ID] {

        private val table: Table[Process] = Table[Process](config.tableName)

        override def processing(id: ID): F[Boolean] = {
          for {
            now <- timer.clock.realTime(TimeUnit.SECONDS).map(Instant.ofEpochSecond).map(Expiration.apply)
            result <- scanamoF(table
              .given((not('id -> id) and not('processId -> config.processorId)) or ('expiredOn >= now and 'status -> Status.processing))
              .put(Process(id, config.processorId, Status.processing, Some(now.plus(config.ttl))))).map(_.fold(_ => false, _ => true))
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
}

class ScanamoF[F[_]] {

  def exec[A](dynamoDb: AmazonDynamoDBAsync)(ops: ScanamoOps[A])(implicit ec: ExecutionContext, F: Async[F]): F[A] = {
    F.async[A] { cb =>
      ScanamoAsync.exec[A](dynamoDb)(ops).onComplete(result => cb(result.toEither))
    }
  }
}