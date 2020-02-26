// package com.ovoenergy.comms.deduplication

// import scala.concurrent.ExecutionContext
// import scala.concurrent.duration._
// import java.util.UUID

// import cats.effect._
// import cats.implicits._

// import com.ovoenergy.comms.dockertestkit.{DynamoDbKit, ManagedContainers}
// import com.ovoenergy.comms.dockertestkit.dynamoDb.DynamoDbClient

// import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType._

// import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
// import org.scalatest.{FlatSpec, Matchers}

// import model._
// class ProcessingStoreSpec
//     extends FlatSpec
//     with Matchers
//     with ScalaCheckDrivenPropertyChecks
//     with DynamoDbClient
//     with DynamoDbKit {

//   override def managedContainers: ManagedContainers = ManagedContainers(dynamoDbContainer)

//   implicit val ec = ExecutionContext.global
//   implicit val contextShift = IO.contextShift(ec)
//   implicit val timer: Timer[IO] = IO.timer(ec)
//   it should "process event for the first time, ignore other before expiration" in {

//     val id = UUID.randomUUID().toString

//     val result = processingStoreResource
//       .use { ps =>
//         for {
//           a <- ps.processing(id).map {
//             case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
//             case _ => "processed"
//           }
//           b <- ps.processing(id).map {
//             case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
//             case _ => "processed"
//           }
//         } yield (a, b)
//       }
//       .unsafeRunSync()

//     val (a, b) = result
//     a shouldBe "nonProcessed"
//     b shouldBe "processed"
//   }

//   it should "process event for the first time, accept other after expiration" in {

//     val id = UUID.randomUUID().toString

//     val result = processingStoreResource
//       .use { ps =>
//         for {
//           a <- ps.processing(id).map {
//             case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
//             case _ => "processed"
//           }
//           _ <- IO.sleep(3.seconds)
//           b <- ps.processing(id).map {
//             case ProcessStatus.NotStarted | ProcessStatus.Expired => "nonProcessed"
//             case _ => "processed"
//           }
//         } yield (a, b)
//       }
//       .unsafeRunSync()

//     val (a, b) = result
//     a shouldBe "nonProcessed"
//     b shouldBe "nonProcessed"
//   }

//   it should "always process event for the first time" in {

//     val id1 = UUID.randomUUID().toString
//     val id2 = UUID.randomUUID().toString

//     val result = processingStoreResource
//       .use { ps =>
//         for {
//           a <- ps.protect(id1, IO("nonProcessed"), IO("processed"))
//           b <- ps.protect(id2, IO("nonProcessed"), IO("processed"))
//         } yield (a, b)
//       }
//       .unsafeRunSync()

//     val (a, b) = result
//     a shouldBe "nonProcessed"
//     b shouldBe "nonProcessed"
//   }

//   val processingStoreResource: Resource[IO, ProcessingStore[IO, String]] = for {
//     processorId <- Resource.liftF(IO(UUID.randomUUID().toString))
//     table <- tableResource[IO]('id -> S, 'processorId -> S)
//     dbClient <- dynamoDbClientResource[IO]()
//   } yield ProcessingStore[IO, String, String](
//     Config(
//       Config.TableName(table.value),
//       processorId,
//       1.second,
//       30.days,
//       PollStrategy.linear()
//     ),
//     dbClient
//   )
// }
