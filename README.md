# Comms-deduplication

> Mnemosyne (mnɛːmosýːnɛː) is the Greek god of memory. "Mnemosyne" is derived from the same source as the word mnemonic, that being the Greek word mnēmē, which means "remembrance, memory"

This library is  responsible to deduplicate sygnals received from external system by remembering which one has been alredy processed.

It is based on thwo main concepts:

- `id`: The unique identifier of the sygnal
- `processorId`: The unique identifier of the system the process the sygnal

It is able to work across multiple node with the same `processorId`. The persistence is based on [DynamoDb](https://aws.amazon.com/dynamodb/) and its strong consistency write capability. The same concept can be applied to [Apache Cassandra](http://cassandra.apache.org/) or any other similar database that provides these two features:

- Strong consistency writes
- Upsert with the previous record values returned

## How does it work

It is based on the two phase commit strategy. It records when the processor starts to process a sygnal and when it completes it. It provides a `protect` method that wraps the effect of sygnal processing to guarantee that it will happen only once for each `processorId`.

The DynamoDb table has this structure:

- `id`: S - The unique identifier of the sygnal
- `processorId`: S - The unique identifier of the processor
- `startedAt`: N - The last datetime the sygnal has been attempted to be processed
- `completedAt`: N - The datetime when the sygnal has been completed
- `expiresOn`: N - The datetime when the sygnal process will expires

Each time a processor with a given `processorId` attempt to process a sygnal identified by `id`, it updates or writes on the table a record with `id`, `processorId`, `startedAt` and `expiresOn`. The `expiresOn` is allows to recover from a sygnal process that has never completed. Its value should be a little bit longher than time needed to process a sygnal.

If the record with given `id` and `processorId` was already present, its value is returned to the library otherwise nothing is returned.

We can have these scenarios:

1) The sygnal has never been processed previously (not previous record found)
2) The sygnal has been already processed previously (`completedAt` is present)
3) The sygnal has started been processing (`completedAt` is absent and `expiresOn` is in the future)
4) The sygnal has been attempted to be processed previously (`completedAt` is absent and `expiresOn` is in the past)

In cases (1) and (4) the library allow the sygnal to be processed. In cases (2) and (3) the library does not allow the sygnal to be process again.

When the sygnal process completes successfully, the record is updated with the `completedAt` time.

As you can notice that `startedAt` end `expiresOn` change at any attempt to process the same sygnal, this is why we cannot have a DynamoDb TTL mechanism in place. This is a tradeoff to avoid using the conditional writes that are ore coslty.
