package duplicates

import java.time.Duration

import cats.effect.Sync
import ciris.aiven.kafka.aivenKafkaSetup
import ciris.credstash.credstashF
import ciris.{envF, loadConfig}
import ciris.syntax._
import ciris.cats.effect._
import com.ovoenergy.kafka.serialization.avro.SchemaRegistryClientSettings
import org.apache.kafka.clients.consumer.ConsumerConfig

case class Config(kafka: KafkaConfig, processingStore: DynamoProcessingStoreConfig[String])

case class DynamoProcessingStoreConfig[ProcessorID](tableName: String, processorId: ProcessorID, ttl: Duration)

case class KafkaConfig(consumerProperties: Map[String, String], schemaRegistry: SchemaRegistryClientSettings, topic: String)

object Config {

  def load[F[_] : Sync]: F[Config] = loadConfig(
    credstashF[F, String]()("uat.aiven.kafka_hosts"),
    credstashF[F, String]()("uat.aiven.schema_registry.url"),
    credstashF[F, String]()("uat.aiven.schema_registry.username"),
    credstashF[F, String]()("uat.aiven.schema_registry.password"),
    aivenKafkaSetup[F](
      credstashF()(s"uat.kafka.client_private_key"),
      credstashF()(s"uat.kafka.client_certificate"),
      credstashF()(s"uat.kafka.service_certificate")
    )
  ) { (kafkaBootstrapServers,
       schemaRegistryEndpoint,
       schemaRegistryUsername,
       schemaRegistryPassword,
       kafkaSetup) =>

    val consumerProperties = kafkaSetup.setProperties(Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> "phil-test-v2",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
    ))((acc, k, v) => acc + (k -> v))

    Config(
      KafkaConfig(
        consumerProperties,
        SchemaRegistryClientSettings(schemaRegistryEndpoint, schemaRegistryUsername, schemaRegistryPassword),
        "comms.triggered.v4"
      ),
      DynamoProcessingStoreConfig[String](
        "phil-processing",
        "phil-test",
        Duration.ofSeconds(5)
      )
    )
  }.orRaiseThrowable

}