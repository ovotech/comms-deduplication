package com.ovoenergy.comms.deduplication
package dynamodb

import java.{util => ju}
import java.time.Instant
import scala.reflect.ClassTag

import cats.implicits._

import software.amazon.awssdk.services.dynamodb.model._

import org.scalacheck.Gen
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._

import Generators._

class DynamoDbCodecSuite extends munit.ScalaCheckSuite {

  checkEncode[String](str => AttributeValue.builder().s(str).build())
  checkEncodeDecode[String]

  checkEncode[Int](int => AttributeValue.builder().n(s"$int").build())
  checkEncodeDecode[Int]

  checkEncode[Long](long => AttributeValue.builder().n(s"$long").build())
  checkEncodeDecode[Long]

  checkEncode[ju.UUID](uuid => AttributeValue.builder().s(uuid.toString).build())
  checkEncodeDecode[ju.UUID]

  checkEncode[Instant](instant => AttributeValue.builder().n(s"${instant.toEpochMilli()}").build())
  checkEncodeDecode[Instant]

  def checkEncode[T: Arbitrary: DynamoDbEncoder](
      expected: T => AttributeValue
  )(implicit loc: munit.Location, classTag: ClassTag[T]): Unit = {
    property(s"should encode ${classTag.runtimeClass}") {
      forAll { t: T =>
        val result = DynamoDbEncoder[T].write(t)
        assertEquals(result, expected(t))
      }
    }
  }

  def checkEncodeDecode[T: Arbitrary: DynamoDbEncoder: DynamoDbDecoder](
      implicit loc: munit.Location,
      classTag: ClassTag[T]
  ): Unit = {
    property(s"should encode/decode ${classTag.runtimeClass}") {
      forAll { t: T =>
        val result = DynamoDbDecoder[T].read(DynamoDbEncoder[T].write(t))
        assertEquals(result, Right(t))
      }
    }
  }

}
