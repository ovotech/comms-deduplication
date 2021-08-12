package com.ovoenergy.comms.deduplication

import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.DeduplicationTestUtils._
import munit._
import scala.concurrent.duration._

class DeduplicationSuite extends FunSuite {

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  test("should run an `F[A]` and return the result") {
    testDeduplication.map(_.context("test")).use { dedup =>
      for {
        p1 <- TestProcess("p1".some, 3.seconds)
        res <- dedup.protect("p1", p1.run)
      } yield {
        assertEquals(clue(res), "p1")
        assert(clue(p1.started))
        assert(clue(p1.completed))
      }
    }
  }

  test("should only let one process start and complete") {
    testDeduplication.map(_.context("test")).use { dedup =>
      for {
        ps <- List.fill(100)(TestProcess("p".some)).sequence
        _ <- ps.parTraverse { p =>
          dedup.protect("id", p.run)
        }
      } yield {
        val started = ps.count(_.started)
        val completed = ps.count(_.completed)
        assertEquals(clue(started), 1)
        assertEquals(clue(completed), 1)
      }
    }
  }

  test("should only allow one process to take over a stale one") {
    testDeduplication.map(_.context("test")).use { dedup =>
      for {
        fail <- TestProcess(none, 2.seconds)
        _ <- dedup.protect("id", fail.run).attempt.start
        _ <- IO.sleep(500.millis)
        ps <- List.fill(100)(TestProcess("takeover".some)).sequence
        results <- ps.parTraverse { p =>
          dedup.protect("id", p.run)
        }
      } yield {
        val started = ps.count(_.started)
        val completed = ps.count(_.completed)
        assert(clue(fail.started))
        assert(!clue(fail.completed))
        assertEquals(clue(started), 1)
        assertEquals(clue(completed), 1)
        assert(results.forall { res => clue(res) == "takeover" })
      }
    }
  }

  test("should return the cached value if a process is a duplicate") {
    testDeduplication.map(_.context("test")).use { dedup =>
      for {
        ps <- List.fill(100)(TestProcess("result".some)).sequence
        results <- ps.parTraverse { p =>
          dedup.protect("id", p.run)
        }
      } yield {
        val started = ps.count(_.started)
        val completed = ps.count(_.completed)
        assertEquals(clue(started), 1)
        assertEquals(clue(completed), 1)
        assert(results.forall { res => clue(res) == "result" })
      }
    }
  }

  // TODO
  // test("should only allow one process to run at any given time") {
  //   testDeduplication.map(_.context("test")).use { dedup =>
  //     for {
  //       ps <- List.fill(100)(TestProcess(none, 500.second)).sequence
  //       results <- ps.parTraverse { p =>
  //         dedup.protect("id", p.run)
  //       }
  //     } yield {
  //       val started = ps.count(_.started)
  //       val completed = ps.count(_.completed)
  //       assertEquals(clue(started), 1)
  //       assertEquals(clue(completed), 1)
  //       assert(results.forall { res => clue(res) == "result" })
  //     }
  //   }
  // }

  // TODO: test maxProcessingTime timeout

}
