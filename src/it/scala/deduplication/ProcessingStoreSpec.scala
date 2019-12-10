package com.ovoenergy.comms.deduplication

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import cats.effect._
import cats.implicits._

import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import model._

class ProcessingStoreSpec
    extends FlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {

    implicit val ec = ExecutionContext.global
    implicit val contextShift = IO.contextShift(ec)
    implicit val timer: Timer[IO] = IO.timer(ec)
    

  it should "process only one event out of multiple concurrent events" in {

    val event = UUID.randomUUID.toString
    val n = 1200
    val processorId = UUID.randomUUID.toString

    val result = processingStoreResource(processorId)
      .use { ps =>
        List.fill(math.abs(n))(event).parTraverse { i =>
          ps.protect(i, IO(1), IO(0))
        }
      }
      .unsafeRunSync()

    result.sum shouldBe 1
  }
  
  def processingStoreResource(processorId: String) = for {
    table <- Resource.liftF(IO(sys.env("TEST_TABLE")))
    processingStore <- ProcessingStore.resource[IO, String, String](Config(
      tableName = Config.TableName(table),
      processorId = processorId, 
      maxProcessingTime = 5.seconds,
      ttl = 1.day,
      pollStrategy = PollStrategy.linear()
    ))
  } yield processingStore
}