# Mnemosyne

> Mnemosyne (mnɛːmosýːnɛː) is the Greek god of memory. "Mnemosyne" is derived
> from the same source as the word mnemonic, that being the Greek word mnēmē,
> which means "remembrance, memory"

Given an effectful operation `F[A]`, this library can be used to wrap the
computation so that it's only executed once[*](#limitations). It achieves this
by remembering which operations have already been executed and what their result
`A` was.

It is based on two main concepts:

- `id`: An identifier of the operation, this can be any value that is both
  consistent and unique for each operation. _Only one[*](#limitations)_
  operation with the same `id` will be allowed to run.
- `contextId`: A context identifier, usually the type of operation being
  performed. This is used to keep different types of operation separate within a
  service, so that they can all use the same `id` (eg: they are all processing
  the same entity)

The library is able to work across multiple nodes with the same `contextId`. The
persistence is based on [DynamoDb](https://aws.amazon.com/dynamodb/) and its
strong write consistency capability. The same concept can be applied to [Apache
Cassandra](http://cassandra.apache.org/) or any other similar database that
provides these two features:

- Strong write consistency
- Upsert with the previous record values returned

## Add the dependency to your project

You'll need to add our public Maven repository:

```scala
resolvers += "Artifactory maven" at "https://kaluza.jfrog.io/artifactory/maven"

```

Then add this snippet to your `build.sbt`

```scala
libraryDependencies += "com.ovoenergy.comms" %% "deduplication" % "$VERSION"
```

An [example terraform file](example.tf) is provided for provisioning the backing
database with DynamoDB.

## How to use it

The main two objects in the library are `Deduplication` and
`DeduplicationContext`.

`Deduplication` is the entrypoint.
It holds the global configuration and a reference to the data storage
and would usually be instantiated once at the beginning of your program.

`DeduplicationContext` is used to deduplicate a single effectful operation.
You can have as many contexts as you like, as long as they all have a different
context ID.
New contexts can be instantiated by calling `.context(<contextId>)` on a
`Deduplication` instance.

Once you have an instance of `DeduplicationContext`, you can use
`context.protect(id: ID, fa: F[A])` to wrap your side effects.

#### DynamoDB backend

In order to create a new `Deduplication` instance you will need an
implementation of `ProcessRepo`, which is the data backend that is responsible
to store information around which operations have been executed and what their
results were.

The library has a built-in implementation of `ProcessRepo` that uses DynamoDB
and [Meteor](https://d2a4u.github.io/meteor/) under the hood. You can use it by
creating an instance of `Deduplication` with the
`com.ovoenergy.comms.deduplication.meteor.MeteorDeduplication` factory object.

If you use the meteor implementation you will need to make sure the meteor
codecs for the return type of your operations are in scope when you instantiate
new contexts.

#### Example

In the following example a service is consuming a stream of events and
performing 2 effectful operations on top of them.

Using the library we make sure[*](#limitations) that, if the same event is
consumed more than once, the side effects will not be re-executed.

```scala
import cats.effect._
import com.ovoenergy.comms.deduplication
import com.ovoenergy.comms.deduplication.meteor.MeteorDeduplication
import com.ovoenergy.comms.deduplication.meteor.codecs._
import meteor.CompositeKeysTable
import meteor.syntax._

// A stream of events.
// This could contain duplicates itself or the same event could be present
// in different streams across service instances
val events: Stream[IO, MyEvent] = ???

// Effectful operations that need to be executed for each event
def sendEmail(evt: MyEvent): IO[String] = ???
def storeEmail(evt: MyEvent, sendId: String): IO[Unit] = ???

// Global configuration
val dedupConf: deduplication.Config = ???
val dedupTable: CompositeKeysTable[String, String] = ???

val dedupResource = for {
  client <- meteor.Client.resource[IO]
  dedup <- MeteorDeduplication.resource[IO, String, String](
    client,
    dedupTable,
    dedupConf
  )
} yield dedup

dedupResource.use { deduplication =>

  // Create two contexts to deduplicate the operations separately
  val sendEmailCtx = deduplication.context[String]("sendEmail")
  val storeEmailCtx = deduplication.context[Unit]("storeEmail")

  events
    .evalMap { evt =>
      for {
        // Wrap the operations in a protect call
        // returns the stored result if sendEmail(evt) was already executed in a different thread or process
        sendId <- sendEmailCtx.protect(evt.id, sendEmail(evt))
        _ <- storeEmailCtx.protect(evt.id, storeEmail(evt, sendId))
      } yield ()
    }
    .compile
    .drain
}
```

## How does it work

The library is based on the two phase commit strategy. It records when the
an operation starts being executed within a context and when it's completed.
 It provides a `protect` method that wraps an effectful operation to
 guarantee[*](#limitations) that it will happen only once for each `contextId`.

The DynamoDb table has this structure:

- `id`: S - The unique identifier of the execution
- `contextId`: S - The unique identifier of the context
- `startedAt`: N - The datetime the signal has started to be processed
- `result`: M - An object containing the result of the execution
- `expiresOn`: N - The datetime when the process result will expire

Each time a context with a given `contextId` attempts to execute an operation
identified by `id`, it updates or writes on the table a record with `id`,
`contextId`, `startedAt`. If a record with the given `id` and `contextId` is
already present, its value is returned to the library, otherwise nothing is
returned. After the operation has run successfully, the library marks it as
completed by storing the `result` and the `expiresOn` fields.

The `expiresOn` allows clean up of old data and the same operation to re-run
after some time.

When the library attempts to start a process, these scenarios can happen:

1. The signal has never been processed previously (no previous record found)
2. The signal has timed out processing (`result` is absent and `startedAt +
processingTime` is in the past)
3. The signal has already been processed previously (`result` is present)
4. The signal is still being processed (`result` is absent and `startedAt +
processingTime` is in the future)

In cases (1) and (2) the library allows the signal to be processed. In case (3)
the library does not allow the signal to be processed again and returns the
stored result. In case (4) the library waits for the process to either complete
or timeout before making any decision.

## How to configure it

A `deduplication.Config` is required in order to create an instance of
`Deduplication`. The following parameters are available:

- `maxProcessingTime: FiniteDuration`: The time after which a pending operation
  will be considered stale and a new one will be allowed to take over.
- `ttl: Option[FiniteDuration]`: The time after which a successful operation
  will be considered expired and allowed to run again. If `None` the operation
  will never expire.
- `pollStrategy: Config.PollStrategy`: The delay strategy to use when polling
  for the status of a running operation. The `PollStrategy` object provides
  helper methods for creating one easily.

A `contextId` needs to be assigned to each instance of `DeduplicationContext`.
It will uniquely identify the type of operation being run.  If more than one
service instance uses the same `contextId` the library will
ensure[*](#limitations) that each operation is only executed once across all of
them.

## Limitations

Unsurprisingly, this library doesn't achieve perfect _exactly once_ executions,
but it makes a best effort at it.

It's purpose is to __limit__ duplication as much as possible while making sure
that all operations are executed _at least once_.

You should still make sure your system is resilient to duplicates.
