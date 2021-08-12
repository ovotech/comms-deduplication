package com.ovoenergy.comms.deduplication

import _root_.meteor._
import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.meteor._
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import software.amazon.awssdk.services.dynamodb.model.BillingMode
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object DeduplicationTestUtils {

  implicit val ec = ExecutionContext.global
  implicit val contextShift = IO.contextShift(ec)
  implicit val timer: Timer[IO] = IO.timer(ec)

  trait TestProcess {
    def started: Boolean
    def completed: Boolean
    def run: IO[String]
  }

  object TestProcess {
    def apply(result: Option[String], delay: FiniteDuration = 0.seconds): IO[TestProcess] =
      IO.delay {
        var hasStarted: Boolean = false
        var hasCompleted: Boolean = false
        new TestProcess {
          def started = hasStarted
          def completed = hasCompleted
          def run =
            IO.delay { hasStarted = true } >>
              IO.sleep(delay) >>
              result.fold(IO.raiseError[String](new Exception("Process failure"))) { res =>
                IO.delay { hasCompleted = true } >>
                  IO.delay(res)
              }
        }
      }
  }

  def createTestTable(client: Client[IO], name: String) = {
    val partitionKey = KeyDef[String]("id", DynamoDbType.S)
    val sortKey = KeyDef[String]("contextId", DynamoDbType.S)
    client
      .createCompositeKeysTable(
        name,
        partitionKey,
        sortKey,
        BillingMode.PAY_PER_REQUEST
      )
      .map(_ => CompositeKeysTable(name, partitionKey, sortKey))
  }

  def deleteTable(client: Client[IO], name: String) =
    client.deleteTable(name)

  val uuidF = IO(UUID.randomUUID())

  val testRepo: Resource[IO, ProcessRepo[IO, String, String, AttributeValue]] =
    for {
      uuid <- Resource.eval(uuidF)
      tableName = s"comms-deduplication-test-${uuid}"
      client <- Client.resource[IO]
      table <- Resource.make[IO, CompositeKeysTable[String, String]](
        IO(println(s"Creating table ${tableName}")) >> createTestTable(client, tableName)
      )(_ => deleteTable(client, tableName))
    } yield MeteorProcessRepo[IO, String, String](client, table, readConsistently = true)

  val testDeduplication: Resource[IO, Deduplication[IO, String, String, AttributeValue]] =
    testRepo.evalMap { repo =>
      val config: Config = Config(
        5.seconds,
        none,
        Config.PollStrategy.linear(1.second, 10.seconds)
      )
      Deduplication.apply(repo, config)
    }

}
