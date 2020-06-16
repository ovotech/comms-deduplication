package com.ovoenergy.comms.deduplication
package dynamodb

import java.time.Instant

import cats._
import cats.implicits._

import software.amazon.awssdk.services.dynamodb.model._

trait DynamoDbEncoder[A] {
  def write(a: A): AttributeValue
}

object DynamoDbEncoder {
  def apply[A](implicit dd: DynamoDbEncoder[A]): DynamoDbEncoder[A] = dd

  def instance[A](f: A => AttributeValue): DynamoDbEncoder[A] = new DynamoDbEncoder[A] {
    def write(a: A): AttributeValue = f(a)
  }

  def const[A](av: AttributeValue): DynamoDbEncoder[A] = new DynamoDbEncoder[A] {
    def write(a: A): AttributeValue = av
  }

  implicit def contravariantForDynamoDbDecoder: Contravariant[DynamoDbEncoder] =
    new Contravariant[DynamoDbEncoder] {
      def contramap[A, B](fa: DynamoDbEncoder[A])(f: B => A): DynamoDbEncoder[B] =
        new DynamoDbEncoder[B] {
          def write(b: B): AttributeValue = fa.write(f(b))
        }
    }

  implicit def dynamoEncoderForOption[A: DynamoDbEncoder]: DynamoDbEncoder[Option[A]] =
    DynamoDbEncoder.instance { fa =>
      fa.fold(AttributeValue.builder().nul(true).build())(DynamoDbEncoder[A].write)
    }

  implicit val dynamoEncoderForBoolean: DynamoDbEncoder[Boolean] =
    DynamoDbEncoder.instance(bool => AttributeValue.builder().bool(bool).build())

  implicit val dynamoEncoderForString: DynamoDbEncoder[String] =
    DynamoDbEncoder.instance(str => AttributeValue.builder().s(str).build())

  implicit val dynamoEncoderForLong: DynamoDbEncoder[Long] =
    DynamoDbEncoder.instance(long => AttributeValue.builder().n(long.toString).build())

  implicit val dynamoEncoderForInt: DynamoDbEncoder[Int] =
    DynamoDbEncoder.instance(int => AttributeValue.builder().n(int.toString).build())

  implicit val dynamoEncoderForInstant: DynamoDbEncoder[Instant] =
    dynamoEncoderForLong.contramap(instant => instant.toEpochMilli())

}
