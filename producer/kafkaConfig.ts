import { Kafka, Partitioners, logLevel } from "kafkajs";
import * as avro from "avsc";
/**
 * The `KafkaConfig` class is responsible for configuring and managing the Kafka client, consumer, and producer.
 */
class KafkaConfig {
      /**
   * Initializes the Kafka client, producer, and consumer with default settings.
   */
  public kafka: any;
  public consumer: any;
  public producer: any;
  constructor() {
    this.kafka = new Kafka({
      clientId: "my-app",
      brokers: ["localhost:9092"],
      // brokers: ['192.168.100.151:9092', '192.168.100.152:9092', '192.168.100.153:9092'],
      connectionTimeout: 3000,
      // logLevel: logLevel.DEBUG
    });
    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
    this.consumer = this.kafka.consumer({
      groupId: "test-group",
      clientId: "my-app",
    });
  }
  /**
   * Connects to the Kafka broker, sends the specified messages to the specified topic, and disconnects from the broker.
   * @param topic - The topic to produce messages to.
   * @param messages - The messages to produce.
   */
  async produce(topic: string, messages: any) {
    try {
      await this.producer.connect();
      await this.producer.send({
        topic,
        messages,
      });
    } catch (error) {
      console.error(error);
    } finally {
      await this.producer.disconnect();
    }
  }
}

export { KafkaConfig };
