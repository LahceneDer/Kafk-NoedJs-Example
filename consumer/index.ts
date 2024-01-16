import express from "express";
import { KafkaConfig } from "./kafkaConfig";

const StartServer = async () => {
  const app = express();
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  const kafkaonfig = new KafkaConfig();

  kafkaonfig.consume("your-topic", (value: any) => {
    console.log(JSON.stringify(value));
  });
  app.listen(3033, () => {
    console.log(`Listening on port ${3033}`);
  });
};

StartServer();
