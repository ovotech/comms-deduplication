package deduplication

import java.time.Duration

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

import scala.concurrent.ExecutionContext

class ProcessingStoreSpec
    extends FlatSpec
    with Matchers
    with GeneratorDrivenPropertyChecks
    with DynamoDbClient
    with DynamoDbKit {

  override def managedContainers: ManagedContainers = ManagedContainers(dynamoDbContainer)

  implicit val ec = ExecutionContext.global
  implicit val contextShift = cats.effect.IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  val tableName: String = "eventIds"
  val arbNonEmptyString = arbitrary[String].suchThat(!_.isEmpty)

  it should "process event for the first time, ignore after that" in forAll(arbNonEmptyString) {
    id: String =>
      tableResource[IO](TableName(tableName), 'id -> S)
        .use { _ =>
          val dynamoDbR: Resource[IO, AmazonDynamoDBAsync] =
            Resource.make(
              Sync[IO].delay(
                AmazonDynamoDBAsyncClientBuilder
                  .standard()
                  .withClientConfiguration(new ClientConfiguration().withProtocol(Protocol.HTTP))
                  .withEndpointConfiguration(
                    new EndpointConfiguration(dynamoDbPublicEndpoint, Regions.EU_WEST_1.getName))
                  .build()))(c => Sync[IO].delay(c.shutdown()))

          val processingStoreR =
            ProcessingStore.fromDynamo[IO, String, String](
              Config(
                Config.TableName(tableName),
                "tester",
                Duration.ofSeconds(1)
              ),
              dynamoDbR
            )

          processingStoreR.use(ps => {
            ps.protect(id, IO("nonProcessed"), IO("processed")).map(_ shouldBe "nonProcessed")
          }) *>
            processingStoreR.use(ps => {
              ps.protect(id, IO("nonProcessed"), IO("processed")).map(_ shouldBe "processed")
            })
        }
        .unsafeRunSync()
  }
}
