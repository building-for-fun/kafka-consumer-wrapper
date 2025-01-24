import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';

export interface KafkaConsumerConfig {
  brokers: string[];
  groupId: string;
  topic: string;
  fromBeginning?: boolean;
  handleMessage: (message: any) => Promise<void>;
}

export class KafkaConsumerWrapper {
  private kafka: Kafka;
  private consumer: Consumer;
  private topic: string;
  private handleMessage: (message: any) => Promise<void>;

  constructor(config: KafkaConsumerConfig) {
    this.kafka = new Kafka({ brokers: config.brokers });
    this.consumer = this.kafka.consumer({ groupId: config.groupId });
    this.topic = config.topic;
    this.handleMessage = config.handleMessage;
  }

  async connect(): Promise<void> {
    await this.consumer.connect();
    console.log(`Connected to Kafka as group: ${this.consumer}`);
  }

  async subscribe(fromBeginning = false): Promise<void> {
    await this.consumer.subscribe({ topic: this.topic, fromBeginning });
    console.log(`Subscribed to topic: ${this.topic}`);
  }

  async run(): Promise<void> {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        try {
          const value = message.value?.toString();
          if (value) {
            await this.handleMessage(JSON.parse(value));
          }
        } catch (err) {
          console.error(`Error processing message: ${err}`);
        }
      },
    });
    console.log(`Consumer is running on topic: ${this.topic}`);
  }

  async disconnect(): Promise<void> {
    await this.consumer.disconnect();
    console.log('Consumer disconnected.');
  }
}
