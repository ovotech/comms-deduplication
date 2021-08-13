package com.ovoenergy.comms.deduplication
package meteor

import _root_.meteor.codec.Codec
import _root_.meteor.syntax._
import com.ovoenergy.comms.deduplication.Generators._
import com.ovoenergy.comms.deduplication.meteor.codecs._
import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import java.{util => ju}
import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import scala.reflect.ClassTag
import software.amazon.awssdk.services.dynamodb.model._

class MeteorCodecSuite extends munit.ScalaCheckSuite {

  checkEncodeDecode[String]
  checkEncodeDecode[Int]
  checkEncodeDecode[Long]
  checkEncodeDecode[ju.UUID]
  checkEncodeDecode[Instant]

  def checkEncodeDecode[T: Arbitrary: Codec](
      implicit loc: munit.Location,
      classTag: ClassTag[T]
  ): Unit = {
    property(s"should encode/decode ${classTag.runtimeClass}") {
      val codec = Codec[Process[T, T, AttributeValue]]
      forAll { proc: Process[T, T, T] =>
        val avProc = proc.copy(
          result = proc.result.map(_.asAttributeValue)
        )
        val result = codec.read(codec.write(avProc))
        assertEquals(result, Right(avProc))
      }
    }
  }

}
