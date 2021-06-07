package com.ovoenergy.comms.deduplication

import java.time.Instant

import org.scalacheck._
import org.scalacheck.Arbitrary._

import model._
import java.time.temporal.ChronoUnit

object Generators {

  implicit val genForInstant: Gen[Instant] = for {
    ms <- Gen.posNum[Long]
  } yield Instant.ofEpochMilli(ms)

  implicit val genForExpiration: Gen[Expiration] = for {
    instant <- arbitrary[Instant]
  } yield Expiration(instant)

  implicit def genForProcess[Id: Arbitrary, ProcessorId: Arbitrary]: Gen[Process[Id, ProcessorId]] =
    for {
      id <- arbitrary[Id]
      processorId <- arbitrary[ProcessorId]
      startedAt <- arbitrary[Instant]
      completedAt <- Gen.option(Gen.choose(500, 5000).map(n => startedAt.plusMillis(n)))
      expiresOn <- Gen.option(
        Gen.choose(7, 90).map(n => startedAt.plus(n, ChronoUnit.DAYS)).map(Expiration(_))
      )
    } yield Process[Id, ProcessorId](
      id = id,
      processorId = processorId,
      startedAt = startedAt,
      completedAt = completedAt,
      expiresOn = expiresOn
    )

  implicit def genForProcessStatus: Gen[ProcessStatus] = Gen.oneOf(
    ProcessStatus.Completed,
    ProcessStatus.Expired,
    ProcessStatus.NotStarted,
    ProcessStatus.Running,
    ProcessStatus.Timeout
  )

  implicit def arbForGen[A](implicit gen: Gen[A]): Arbitrary[A] = Arbitrary(gen)
}
