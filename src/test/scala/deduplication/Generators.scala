package com.ovoenergy.comms.deduplication

import com.ovoenergy.comms.deduplication.model._
import java.time.Instant
import java.time.temporal.ChronoUnit
import org.scalacheck.Arbitrary._
import org.scalacheck._

object Generators {

  implicit val genForInstant: Gen[Instant] = for {
    ms <- Gen.posNum[Long]
  } yield Instant.ofEpochMilli(ms)

  implicit val genForExpiration: Gen[Expiration] = for {
    instant <- arbitrary[Instant]
  } yield Expiration(instant)

  implicit def genForProcess[Id: Arbitrary, ContextId: Arbitrary, A: Arbitrary]
      : Gen[Process[Id, ContextId, A]] =
    for {
      id <- arbitrary[Id]
      contextId <- arbitrary[ContextId]
      result <- Gen.option(arbitrary[A])
      startedAt <- arbitrary[Instant]
      expiresOn <- Gen.option(
        Gen.choose(7, 90).map(n => startedAt.plus(n.toLong, ChronoUnit.DAYS))
      )
    } yield Process[Id, ContextId, A](
      id = id,
      contextId = contextId,
      startedAt = startedAt,
      result = result,
      expiresOn = expiresOn
    )

  implicit def genForProcessStatus[A: Arbitrary]: Gen[ProcessStatus[A]] = Gen.oneOf(
    arbitrary[A].map(ProcessStatus.Completed(_)),
    arbitrary[Instant].map(ProcessStatus.Expired[A](_)),
    Gen.const(ProcessStatus.NotStarted[A]()),
    Gen.const(ProcessStatus.Running[A]()),
    arbitrary[Instant].map(ProcessStatus.Timeout[A](_))
  )

  implicit def arbForGen[A](implicit gen: Gen[A]): Arbitrary[A] = Arbitrary(gen)
}
