import { KafkaConfig } from "./kafkaConfig";
import * as avro from "avsc";

/**
 * Sends a message to a Kafka topic using Avro serialization.
 * @param req - The HTTP request object.
 * @param res - The HTTP response object.
 */
export const sendMessage = async (req: any, res: any) => {
  try {
    const { message } = req.body;

    const avroSchema = avro.Type.forSchema({
      type: "record",
      name: "Message",
      fields: [
        { name: "id", type: "string" },
        { name: "value", type: "double" },
      ],
    });

    const avroType = avro.Type.forSchema(avroSchema);
    const avroBuffer = avroType.toBuffer(message);
    const messages = [{ value: avroBuffer }];

    const kafkaConfig = new KafkaConfig();
    const kafkaResponse = await kafkaConfig.produce("your-topic", messages);

    res.json(kafkaResponse);
  } catch (error) {
    console.log(error);
  }
};
