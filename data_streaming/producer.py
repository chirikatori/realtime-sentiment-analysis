from kafka import KafkaProducer
from data_streaming import Config, ScrapeData
import json


class Producer:
    def __init__(self,
                 scraped_data: ScrapeData,
                 topic: str = Config.kafka_topic,
                 bootstrap_servers: str = Config.bootstrap_servers):
        self.scraped_data = scraped_data
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

    def send_messages(self):
        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        try:
            for message in self.scraped_data:
                print(f"Sending message: {message}")
                producer.send(self.topic, value={"message": message})

            producer.flush()
            print("All messages sent successfully!")
        except Exception as e:
            print(f"Error while sending messages: {e}")
        finally:
            producer.close()
