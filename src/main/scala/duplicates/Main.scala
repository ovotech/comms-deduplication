package duplicates

import com.ovoenergy.comms.model._
import com.ovoenergy.kafka.serialization.core._
import com.ovoenergy.kafka.serialization.avro4s._

import cats.effect.{IO, ExitCode, IOApp}
import cats.data.NonEmptyList
import cats.implicits._

import fs2.kafka._

import scala.concurrent.ExecutionContext.Implicits.global

object Main extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    Config.load[IO].flatMap { config =>
      ProcessingStore.fromDynamo[IO, String, String](config.processingStore).use { processingStore =>
        val stream = consumerExecutionContextStream[IO].flatMap { consumerEc =>

          val valueDeserializer = avroBinarySchemaIdWithReaderSchemaDeserializer[TriggeredV4](config.kafka.schemaRegistry, isKey = false, includesFormatByte = true)
          val keyDeserializer = nullDeserializer[String]

          consumerStream[IO].using(ConsumerSettings[String, TriggeredV4](keyDeserializer, valueDeserializer, consumerEc).withProperties(config.kafka.consumerProperties)).flatMap { consumer =>
            consumer.subscribe(NonEmptyList.one(config.kafka.topic)) >>
              consumer
                .stream
                .mapAsync(5) { message =>
                  processingStore.protect(
                    message.record.value().metadata.eventId,
                    IO(println(s"Processing message: ${message.record.value.metadata.eventId}")),
                    IO(println(s"Dropping message: ${message.record.value.metadata.eventId}"))
                  )
                }
          }
        }

        stream.compile.drain.as(ExitCode.Error)
      }
    }
  }
}
