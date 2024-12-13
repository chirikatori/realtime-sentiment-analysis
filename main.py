from data_streaming import (
    Producer,
    Consumer,
    ScrapeData
)


if __name__ == "__main__":
    data = ScrapeData()
    data = data.get_comments()

    producer = Producer(scraped_data=data)
    producer.send_messages()

    consumer = Consumer()
    consumer.start_stream()
