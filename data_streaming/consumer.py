from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType
from data_streaming import Config


class Consumer():
    def __init__(self,
                 schema: StructType = None,
                 appName: str = Config.appName,
                 kafka_brokers: str = Config.kafka_brokers,
                 kafka_topic: str = Config.kafka_topic,
                 ):
        self.kafka_brokers = kafka_brokers
        self.kafka_topic = kafka_topic
        self.schema = schema
        self.spark = SparkSession.builder.appName(appName) \
            .getOrCreate()
        if self.schema is None:
            raise ValueError("Schema is None, Schema must be StructType based on your producer data") # noqa

    def start_stream(self):
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json")
        kafka_df = kafka_df.select(from_json(col("json"), self.schema).alias(
            "data")).select("data.*")
        query = kafka_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        query.awaitTermination()
