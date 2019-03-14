package com.ovoenergy.comms.deduplication

import java.time.Duration
import scala.concurrent.ExecutionContext

import cats.effect.{IO, Resource, Sync, Timer}
import cats.implicits._

import com.ovoenergy.comms.dockertestkit.{DynamoDbKit, ManagedContainers}
import com.ovoenergy.comms.dockertestkit.dynamoDb.DynamoDbClient
import com.ovoenergy.comms.dockertestkit.Model.TableName

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._
import com.amazonaws.{ClientConfiguration, Protocol}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder}

import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

class ProcessingStoreSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with DynamoDbClient
    with DynamoDbKit {

  override def managedContainers: ManagedContainers = ManagedContainers(dynamoDbContainer)

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  it should "process event for the first time, ignore after that" in {

    val id = "test"

    val result = processingStoreResource
      .use { ps =>
        for {
          a <- ps.protect(id, IO("nonProcessed"), IO("processed"))
          b <- ps.protect(id, IO("nonProcessed"), IO("processed"))
        } yield (a, b)
      }
      .unsafeRunSync()

    val (a, b) = result
    a shouldBe "nonProcessed"
    b shouldBe "processed"
  }

  it should "always process event for the first time" in {

    val id1 = "test-1"
    val id2 = "test-2"

    val result = processingStoreResource
      .use { ps =>
        for {
          a <- ps.protect(id1, IO("nonProcessed"), IO("processed"))
          b <- ps.protect(id2, IO("nonProcessed"), IO("processed"))
        } yield (a, b)
      }
      .unsafeRunSync()

    val (a, b) = result
    a shouldBe "nonProcessed"
    b shouldBe "nonProcessed"
  }

  val processingStoreResource: Resource[IO, ProcessingStore[IO, String]] = for {
    table <- tableResource[IO]('id -> S)
    dbClient <- dynamoDbClientResource[IO]()
  } yield
    ProcessingStore[IO, String, String](
      Config(
        Config.TableName(table.value),
        "tester",
        Duration.ofSeconds(1)
      ),
      dbClient
    )
}
