package com.ovoenergy.comms.deduplication.meteor

import cats.implicits._
import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import meteor.codec.Codec
import meteor.errors._
import meteor.syntax._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

package object codecs {

  object fields {
    val id = "id"
    val contextId = "contextId"
    val startedAt = "startedAt"
    val expiresOn = "expiresOn"
    val result = "result"
  }

  implicit def ProcessCodec[ID: Codec, ContextID: Codec, A: Codec]
      : Codec[Process[ID, ContextID, A]] =
    new Codec[Process[ID, ContextID, A]] {
      def read(av: AttributeValue): Either[DecoderError, Process[ID, ContextID, A]] =
        (
          av.getAs[ID](fields.id),
          av.getAs[ContextID](fields.contextId),
          av.getAs[Instant](fields.startedAt),
          av.getOpt[Instant](fields.expiresOn),
          av.getOpt[A](fields.result)
        ).mapN(Process.apply _)
      def write(process: Process[ID, ContextID, A]): AttributeValue =
        Map(
          fields.id -> process.id.asAttributeValue,
          fields.contextId -> process.contextId.asAttributeValue,
          fields.startedAt -> process.startedAt.asAttributeValue,
          fields.expiresOn -> process.expiresOn.asAttributeValue,
          fields.result -> process.result.asAttributeValue
        ).asAttributeValue
    }

}
