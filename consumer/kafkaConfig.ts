import { Kafka, Partitioners, logLevel } from "kafkajs";
import * as avro from "avsc";

class KafkaConfig {
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

  async produce(topic: string, messages: any) {
    try {
      await this.producer.connect();
      const avroSchema = avro.Type.forSchema({
        type: "record",
        name: "Message",
        fields: [{ name: "test", type: "int" }],
      });
      const avroType = avro.Type.forSchema(avroSchema);
      const avroMessage = {
        id: "123",
        value: 45.67,
      };
      const avroBuffer = avroType.toBuffer(avroMessage);
      //    await this.producer.send({
      //     topic, messages
      //    })
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

  async consume(topic: string, callback: any) {
    const avroSchema = avro.Type.forSchema({
      type: "record",
      name: "Message",
      fields: [
        { name: "id", type: "string" },
        { name: "value", type: "double" },
      ],
    });
    const avroType = avro.Type.forSchema(avroSchema);
    try {
      await this.consumer.connect();
      await this.consumer.subscribe({ topic, fromBeginning: true });
      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }: any) => {
          const jsonMessage = avroType.fromBuffer(message.value);
          // console.log(`Received message from topic ${topic}, partition ${partition}:`, jsonMessage);
          // const value =avroType.fromBuffer(msg);
          callback(jsonMessage);
        },
      });
    } catch (error) {
      console.error(error);
    }
  }
}

export { KafkaConfig };
