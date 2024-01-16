import express from "express";
import { sendMessage } from "./controller";
import { KafkaConfig } from "./kafkaConfig";

const StartServer = async () => {
  const app = express();
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  const kafkaonfig = new KafkaConfig();
  app.post("/send", sendMessage);

  // kafkaonfig.consume('your-topic', (value: any) => {
  //   console.log(value);
  // })
  app.listen(3030, () => {
    console.log(`Listening on port ${3030}`);
  });
};

StartServer();
