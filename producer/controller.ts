import { KafkaConfig } from "./kafkaConfig";
import * as avro from "avsc";

export const sendMessage = async (req: any, res: any) => {
  try {
    const { message } = req.body;
    const kafkaConfig = new KafkaConfig();
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

    const k = await kafkaConfig.produce("your-topic", messages);
    res.json(k);
  } catch (error) {
    console.log(error);
  }
};
