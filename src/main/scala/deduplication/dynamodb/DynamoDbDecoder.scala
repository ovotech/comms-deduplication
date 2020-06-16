package com.ovoenergy.comms.deduplication
package dynamodb

import java.time.Instant

import scala.collection.JavaConverters._

import cats._
import cats.implicits._

import software.amazon.awssdk.services.dynamodb.model._

import scala.annotation.tailrec

case class DecoderFailure(message: String, cause: Option[Throwable] = None) extends RuntimeException(message, cause.getOrElse(null))

trait DynamoDbDecoder[A] {
  def read(av: AttributeValue): Either[DecoderFailure, A]
}

object DynamoDbDecoder {
  
  def apply[A](implicit dd: DynamoDbDecoder[A]): DynamoDbDecoder[A] = dd

  def instance[A](f: AttributeValue => Either[DecoderFailure, A]): DynamoDbDecoder[A] =
    new DynamoDbDecoder[A] {
      def read(av: AttributeValue): Either[DecoderFailure, A] = f(av)
    }

  def failed[A](failure: DecoderFailure): DynamoDbDecoder[A] = new DynamoDbDecoder[A] {
    def read(av: AttributeValue): Either[DecoderFailure, A] = failure.asLeft[A]
  }

  def const[A](a: A): DynamoDbDecoder[A] = new DynamoDbDecoder[A] {
    def read(av: AttributeValue): Either[DecoderFailure, A] = a.asRight[DecoderFailure]
  }

  implicit def monadForDynamoDbDecoder: Monad[DynamoDbDecoder] = new Monad[DynamoDbDecoder] {

    def pure[A](x: A): DynamoDbDecoder[A] = DynamoDbDecoder.instance[A](_ => x.asRight[DecoderFailure])

    def flatMap[A, B](fa: DynamoDbDecoder[A])(f: A => DynamoDbDecoder[B]): DynamoDbDecoder[B] =
      new DynamoDbDecoder[B] {
        def read(av: AttributeValue): Either[DecoderFailure, B] = fa.read(av).flatMap(a => f(a).read(av))
      }

    def tailRecM[A, B](init: A)(f: A => DynamoDbDecoder[Either[A, B]]): DynamoDbDecoder[B] =
      new DynamoDbDecoder[B] {
        @tailrec
        private def step(av: AttributeValue, a: A): Either[DecoderFailure, B] = f(a).read(av) match {
          case l @ Left(_) => l.rightCast[B]
          case Right(Left(a2)) => step(av, a2)
          case Right(Right(b)) => Right(b)
        }

        def read(av: AttributeValue): Either[DecoderFailure, B] = step(av, init)
      }
  }

  implicit val dynamoDecoderForAttributeValue: DynamoDbDecoder[AttributeValue] = DynamoDbDecoder.instance(av => av.asRight[DecoderFailure])

  implicit def dynamoDecoderForOption[A: DynamoDbDecoder]: DynamoDbDecoder[Option[A]] =
    DynamoDbDecoder.instance { av =>
      if (Option(av.nul()).map(_.booleanValue()).getOrElse(false)) {
        none[A].asRight[DecoderFailure]
      } else {
        DynamoDbDecoder[A].read(av).map(_.some)
      }
    }

  implicit val dynamoDecoderForString: DynamoDbDecoder[String] = DynamoDbDecoder.instance { av =>
    Option(av.s()).toRight(new DecoderFailure(s"The attribute value must be an S"))
  }

  implicit val dynamoDecoderForBoolean: DynamoDbDecoder[Boolean] = DynamoDbDecoder.instance { av =>
    Option(av.bool()).map(_.booleanValue()).toRight(new DecoderFailure(s"The attribute value must be an B"))
  }

  implicit val dynamoDecoderForLong: DynamoDbDecoder[Long] = DynamoDbDecoder.instance { av =>
    Option(av.n())
      .toRight(new DecoderFailure(s"The attribute value must be an N"))
      .flatMap(n => Either.catchNonFatal(n.toLong).leftMap(e => DecoderFailure(e.getMessage(), e.some)))
  }

  implicit val dynamoDecoderForInt: DynamoDbDecoder[Int] = DynamoDbDecoder.instance { av =>
    Option(av.n())
      .toRight(new DecoderFailure(s"The attribute value must be an N"))
      .flatMap(n => Either.catchNonFatal(n.toInt).leftMap(e => DecoderFailure(e.getMessage(), e.some)))
  }

  implicit val dynamoDecoderForInstant: DynamoDbDecoder[Instant] = dynamoDecoderForLong.flatMap { ms =>
    Either.catchNonFatal(Instant.ofEpochMilli(ms)).fold(
      e => DynamoDbDecoder.failed(DecoderFailure(e.getMessage(), e.some)),
      ok => DynamoDbDecoder.const(ok)
    )
  }

  implicit def dynamoDecoderForMap[A: DynamoDbDecoder]: DynamoDbDecoder[Map[String, A]] = instance { av =>
    if(av.hasM()) {
      av.m().asScala.map { case (k, v) =>
        (k, DynamoDbDecoder[A].read(v))
      }.foldLeft(Map.empty[String, A].asRight[DecoderFailure]){(fs, fx) =>
        for {
          s <- fs
          x <- fx._2
        } yield s + (fx._1 -> x)
      }
    } else {
      DecoderFailure("The AttributeValue must be an M").asLeft[Map[String, A]]
    }
  }

}

