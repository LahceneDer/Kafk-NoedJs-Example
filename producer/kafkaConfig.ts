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

  // async consume(topic: string, callback: any) {
  //     try {
  //         await this.consumer.connect({groupId: "test-group"})
  //         await this.consumer.subscribe({ topic, fromBeginning: true })
  //         await this.consumer.run({
  //             eachMessage: async ({ topic, partition, message }: any) => {
  //               console.log({
  //                 value: message.value.toString(),
  //               })
  //             },
  //           })
  //     } catch (error) {
  //         console.error(error);

  //     }
  // }
}

export { KafkaConfig };
