import { Kafka, Partitioners, logLevel } from "kafkajs";
import * as avro from "avsc";

/**
 * The `KafkaConfig` class is responsible for configuring and interacting with a Kafka cluster.
 * It provides methods for producing and consuming messages using Avro serialization.
 */
class KafkaConfig {
  private kafka: any;
  private consumer: any;
  private producer: any;

  /**
   * Initializes the KafkaConfig class with default configuration.
   */
  constructor() {
    this.kafka = new Kafka({
      clientId: 'my-app',
      brokers: ['localhost:9092'],
      connectionTimeout: 3000,
    });

    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });

    this.consumer = this.kafka.consumer({
      groupId: 'test-group',
      clientId: 'my-app',
    });
  }

  /**
   * Connects to the Kafka cluster, serializes the messages using Avro, and sends them to the specified topic.
   * @param topic - The topic to produce messages to.
   * @param messages - The messages to produce.
   */
  async produce(topic: string, messages: any) {
    try {
      await this.producer.connect();

      const avroSchema = avro.Type.forSchema({
        type: 'record',
        name: 'Message',
        fields: [{ name: 'test', type: 'int' }],
      });

      const avroType = avro.Type.forSchema(avroSchema);
      const avroMessage = {
        id: '123',
        value: 45.67,
      };

      const avroBuffer = avroType.toBuffer(avroMessage);

      await this.producer.send({
        topic,
        messages: [{ value: avroBuffer }],
      });
    } catch (error) {
      console.error(error);
    } finally {
      await this.producer.disconnect();
    }
  }

  /**
   * Connects to the Kafka cluster, subscribes to the specified topic, and consumes Avro serialized messages.
   * The callback function is called for each consumed message.
   * @param topic - The topic to consume messages from.
   * @param callback - The callback function to be called for each consumed message.
   */
  async consume(topic: string, callback: any) {
    const avroSchema = avro.Type.forSchema({
      type: 'record',
      name: 'Message',
      fields: [
        { name: 'id', type: 'string' },
        { name: 'value', type: 'double' },
      ],
    });

    const avroType = avro.Type.forSchema(avroSchema);

    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic, fromBeginning: true });
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }: any) => {
          const jsonMessage = avroType.fromBuffer(message.value);
          callback(jsonMessage);
        },
      });
    } catch (error) {
      console.error(error);
    }
  }
}

export default KafkaConfig;

export { KafkaConfig };
