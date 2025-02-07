import { Kafka, Partitioners } from "kafkajs";
import readline from "readline";

const kafka = new Kafka({
  clientId: "bone-app",
  brokers: ["10.0.0.180:9092"],
});

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner,
  });

  console.log("Connecting Producer");
  await producer.connect();
  console.log("Producer Connected Successfully");

  rl.setPrompt("> ");
  rl.prompt();

  rl.on("line", async function (line) {
    await producer.send({
      topic: "bone-topic",
      messages: [
        {
          value: line,
        },
      ],
    });
  }).on("close", async () => {
    await producer.disconnect();
  });
}

init();
