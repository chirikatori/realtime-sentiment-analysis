from data_streaming import (
    Producer,
    Consumer,
    ScrapeData
)
from pyspark.sql.types import StructType, StringType
# import os
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 pyspark-shell' # noqa


if __name__ == "__main__":
    data = ScrapeData()
    data = data.get_comments()

    producer = Producer(scraped_data=data)
    producer.send_messages()

    schema = StructType() \
        .add("comment", StringType())
    consumer = Consumer(schema=schema)
    consumer.start_stream()
