package com.ovoenergy.comms.deduplication
package meteor

import _root_.meteor.codec.Codec
import _root_.meteor.errors._
import _root_.meteor.syntax._
import cats.implicits._
import com.ovoenergy.comms.deduplication.ResultCodec
import com.ovoenergy.comms.deduplication.meteor.model._
import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

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

  implicit val EncodedResultMeteorCodec =
    new Codec[EncodedResult] {
      def read(av: AttributeValue): Either[DecoderError, EncodedResult] =
        av.get("value").map(EncodedResult.apply)
      def write(res: EncodedResult): AttributeValue =
        Map(
          "value" -> res.value
        ).asAttributeValue
    }

  implicit def resultCodecFromMeteorCodec[A](
      implicit meteorCodec: Codec[A]
  ): ResultCodec[EncodedResult, A] =
    new ResultCodec[EncodedResult, A] {
      def read(res: EncodedResult): Either[Throwable, A] = meteorCodec.read(res.value)
      def write(a: A): Either[Throwable, EncodedResult] =
        EncodedResult(meteorCodec.write(a)).asRight[Throwable]
    }

  implicit val UnitResultCodec =
    new ResultCodec[EncodedResult, Unit] {
      def read(a: EncodedResult): Either[Throwable, Unit] = ().asRight[Throwable]
      def write(b: Unit): Either[Throwable, EncodedResult] =
        EncodedResult(
          AttributeValue.builder().nul(true).build()
        ).asRight[Throwable]
    }

  implicit def OptionResultCodec[A: Codec] =
    new ResultCodec[EncodedResult, Option[A]] {
      def read(encoded: EncodedResult): Either[Throwable, Option[A]] = encoded.value.asOpt[A]
      def write(a: Option[A]): Either[Throwable, EncodedResult] =
        EncodedResult(a.asAttributeValue).asRight[Throwable]
    }

  implicit def ProcessDecoder[ID: Codec, ContextID: Codec]
      : Codec[Process[ID, ContextID, EncodedResult]] =
    new Codec[Process[ID, ContextID, EncodedResult]] {
      def read(av: AttributeValue): Either[DecoderError, Process[ID, ContextID, EncodedResult]] =
        (
          av.getAs[ID](fields.id),
          av.getAs[ContextID](fields.contextId),
          av.getAs[Instant](fields.startedAt),
          av.getOpt[Instant](fields.expiresOn),
          av.getOpt[EncodedResult](fields.result)
        ).mapN(Process.apply _)

      def write(process: Process[ID, ContextID, EncodedResult]): AttributeValue =
        Map(
          fields.id -> process.id.asAttributeValue,
          fields.contextId -> process.contextId.asAttributeValue,
          fields.startedAt -> process.startedAt.asAttributeValue,
          fields.expiresOn -> process.expiresOn.asAttributeValue,
          fields.result -> process.result.asAttributeValue
        ).asAttributeValue
    }

}
