package com.ovoenergy.comms.deduplication.meteor

import cats.implicits._
import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import meteor.codec.Codec
import meteor.errors._
import meteor.syntax._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import com.ovoenergy.comms.deduplication.ResultCodec

package object codecs {

  object fields {
    val id = "id"
    val contextId = "contextId"
    val startedAt = "startedAt"
    val expiresOn = "expiresOn"
    val result = "result"
  }

  // Meteor represents Instants as millisecond timestamps by default.
  // That could cause Long overflow errors for dates __very__ far in the future.
  // Use a string ISO-8601 representation instead
  implicit val InstantCodec: Codec[Instant] =
    new Codec[Instant] {
      def read(av: AttributeValue): Either[DecoderError, Instant] =
        av.as[String].flatMap { str =>
          Either
            .catchNonFatal(Instant.parse(str))
            .left
            .map(err => DecoderError(err.getMessage(), err.getCause().some))
        }

      def write(a: Instant): AttributeValue = a.toString().asAttributeValue

    }

  implicit def resultCodecFromMeteorCodec[A](
      implicit meteorCodec: Codec[A]
  ): ResultCodec[AttributeValue, A] =
    new ResultCodec[AttributeValue, A] {
      def read(av: AttributeValue): Either[Throwable, A] = meteorCodec.read(av)
      def write(a: A): Either[Throwable, AttributeValue] = meteorCodec.write(a).asRight[Throwable]
    }

  implicit def ProcessCodec[ID: Codec, ContextID: Codec]
      : Codec[Process[ID, ContextID, AttributeValue]] =
    new Codec[Process[ID, ContextID, AttributeValue]] {
      def read(av: AttributeValue): Either[DecoderError, Process[ID, ContextID, AttributeValue]] =
        (
          av.getAs[ID](fields.id),
          av.getAs[ContextID](fields.contextId),
          av.getAs[Instant](fields.startedAt),
          av.getOpt[Instant](fields.expiresOn),
          av.getOpt[AttributeValue](fields.result)
        ).mapN(Process.apply _)
      def write(process: Process[ID, ContextID, AttributeValue]): AttributeValue =
        Map(
          fields.id -> process.id.asAttributeValue,
          fields.contextId -> process.contextId.asAttributeValue,
          fields.startedAt -> process.startedAt.asAttributeValue,
          fields.expiresOn -> process.expiresOn.asAttributeValue,
          fields.result -> process.result.asAttributeValue
        ).asAttributeValue
    }

}
