# Mnemosyne

> Mnemosyne (mnɛːmosýːnɛː) is the Greek god of memory. "Mnemosyne" is derived from the same source as the word mnemonic, that being the Greek word mnēmē, which means "remembrance, memory"

This library is responsible for deduplicating signals received from an external system by remembering which have already been processed.

It is based on two main concepts:

- `id`: The unique identifier of the signal
- `processorId`: The unique identifier of the system that processes the signal

It is able to work across multiple nodes with the same `processorId`. The persistence is based on [DynamoDb](https://aws.amazon.com/dynamodb/) and its strong write consistency capability. The same concept can be applied to [Apache Cassandra](http://cassandra.apache.org/) or any other similar database that provides these two features:

- Strong write consistency
- Upsert with the previous record values returned

## Usage

You'll need to add our public Maven repository:

```scala
resolvers += "Artifactory maven" at "https://kaluza.jfrog.io/artifactory/maven"

```

Then add this snippet to your `build.sbt`

```scala
libraryDependencies += "com.ovoenergy.comms" %% "deduplication" % "$VERSION"
```

An [example terraform file](example.tf) is provided for provisioning the backing database with DynamoDB.

## How to configure it

A `processorId` needs to be assigned to each instance of this library. It will uniquely identify the processor. If two services have the same `processorId` it likely means they are two instances of the same service.

We need to know the max amount of time the process will take (`maxProcessingTime` in the config). Any process that take more than this amount of time will be considered dead.

## How does it work

It is based on the two phase commit strategy. It records when the processor starts to process a signal and when it completes it. It provides a `protect` method that wraps the effect of signal processing to guarantee that it will happen only once for each `processorId`.

The DynamoDb table has this structure:

- `id`: S - The unique identifier of the signal
- `processorId`: S - The unique identifier of the processor
- `startedAt`: N - The datetime the signal has started to be processed
- `completedAt`: N - The datetime when the signal has been completed
- `expiresOn`: N - The datetime when the signal process will expire

Each time a processor with a given `processorId` attempts to process a signal identified by `id`, it updates or writes on the table a record with `id`, `processorId`, `startedAt`. If a record with the given `id` and `processorId` is already present, its value is returned to the library, otherwise nothing is returned. After the process has run successfully, the library marks it as completed by storing the `completedAt` and the `expiresOn` fields.

The `expiresOn` allows clean up of old data and duplicate re-runs after some time.

When the library attempts to start a process, these scenarios can happen:

1) The signal has never been processed previously (no previous record found)
2) The signal has timed out processing (`completedAt` is absent and `startedAt` + `processingTime` is in the past)
3) The signal has already been processed previously (`completedAt` is present)
4) The signal is still being processed (`completedAt` is absent and `startedAt` + `processingTime` is in the future)

In cases (1) and (2) the library allows the signal to be processed. In case (3) the library does not allow the signal to be processed again and in (4) the library waits for the process to either complete or timeout before making any decision.
