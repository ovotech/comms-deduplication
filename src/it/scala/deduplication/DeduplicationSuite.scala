package com.ovoenergy.comms.deduplication

import cats.effect._
import cats.implicits._
import com.ovoenergy.comms.deduplication.DeduplicationTestUtils._
import com.ovoenergy.comms.deduplication.meteor.codecs._
import java.util.concurrent.TimeoutException
import munit._
import scala.concurrent.duration._

class DeduplicationSuite extends FunSuite {

  override def munitValueTransforms = super.munitValueTransforms ++ List(
    new ValueTransform("IO", {
      case io: IO[_] => io.unsafeToFuture()
    })
  )

  test("should run an `F[A]` and return the result") {
    testDeduplication().map(_.context[String]("test")).use { dedup =>
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
    testDeduplication().map(_.context[String]("test")).use { dedup =>
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
    testDeduplication().map(_.context[String]("test")).use { dedup =>
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
    testDeduplication().map(_.context[String]("test")).use { dedup =>
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

  test("should only allow one process to run at any given time") {
    val testPollStrategy = Config.PollStrategy.linear(50.millis, 2.minutes)
    val maxProcessingTime = 100.millis
    val sampleLength = 50
    testDeduplication(
      maxProcessingTime = maxProcessingTime,
      pollStrategy = testPollStrategy
    ).map(_.context[String]("test")).use { dedup =>
      for {
        ps <- List.fill(sampleLength)(TestProcess(none, 10.millis)).sequence
        _ <- ps.parTraverse { p =>
          dedup.protect("id", p.run).attempt
        }
      } yield {
        val started = ps.count(_.started)
        val completed = ps.count(_.completed)
        val timestamps = ps.flatMap(_.startedAt).map(_.toEpochMilli()).sorted
        val timeDiffs = timestamps.zip(timestamps.tail).map {
          case (previous, following) => following - previous
        }
        assertEquals(clue(started), clue(sampleLength))
        assertEquals(clue(completed), 0)
        assertEquals(clue(timestamps.length), clue(sampleLength))
        assert(clue(timeDiffs.sum[Long] / timeDiffs.length) >= clue(maxProcessingTime.toMillis))
      }
    }
  }

  test("should rerun a process if the previous one expired") {
    testDeduplication(ttl = 10.millis.some).map(_.context[String]("test")).use { dedup =>
      for {
        proc1 <- TestProcess("p1".some)
        proc2 <- TestProcess("p2".some)
        res1 <- dedup.protect("id", proc1.run)
        res2 <- dedup.protect("id", proc2.run)
      } yield {
        assert(proc1.started)
        assert(proc1.completed)
        assert(proc2.started)
        assert(proc2.completed)
        assertEquals(clue(res1), "p1")
        assertEquals(clue(res2), "p2")
      }
    }
  }

  test("should time out after `PollStrategy.maxPollDuration` time") {
    val strategy = Config.PollStrategy.linear(50.millis, 500.millis)
    testDeduplication(pollStrategy = strategy, maxProcessingTime = 5.seconds)
      .map(_.context[String]("test"))
      .use { dedup =>
        for {
          halt <- TestProcess(none)
          proc <- TestProcess("result".some)
          protect1 <- dedup.protect("id", halt.run).attempt
          protect2 <- dedup.protect("id", proc.run).attempt
        } yield {
          assert(halt.started)
          assert(!halt.completed)
          assert(!proc.started)
          assert(!proc.completed)
          assert(clue(protect1).left.exists(_ == TestProcess.NoValue))
          assert(clue(protect2).left.exists(_.isInstanceOf[TimeoutException]))
        }
      }
  }

}
