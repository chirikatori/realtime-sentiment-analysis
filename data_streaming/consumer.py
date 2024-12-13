from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.ml import PipelineModel
from data_streaming import Config
import gradio as gr
from thread import Thread
import re


schema = StructType([
    StructField("comment", StringType(), True)
])


class Consumer():
    def __init__(self,
                 schema: StructType = schema,
                 model_path: str = "sentiment_model",
                 appName: str = Config.appName,
                 kafka_brokers: str = Config.kafka_brokers,
                 kafka_topic: str = Config.kafka_topic,
                 ):
        self.kafka_brokers = kafka_brokers
        self.kafka_topic = kafka_topic
        self.schema = schema
        self.spark = SparkSession.builder.appName(appName) \
            .getOrCreate()
        self.pipeline = PipelineModel.load(model_path)

        if self.schema is None:
            raise ValueError("Schema is None, Schema must be StructType based on your producer data") # noqa

    @staticmethod
    def clean_text(text):
        if text is not None:
            # Remove links starting with https://, http://, www., or containing .com  # noqa
            text = re.sub(r'https?://\S+|www\.\S+|\.com\S+|youtu\.be/\S+', '',
                          text)
            # Remove words starting with # or @
            text = re.sub(r'(@|#)\w+', '', text)
            # Convert to lowercase
            text = text.lower()
            # Remove non-alphanumeric characters
            text = re.sub(r'[^a-zA-Z\s]', '', text)
            # Remove extra whitespaces
            text = re.sub(r'\s+', ' ', text).strip()
            return text
        else:
            return ''

    def start_stream(self):
        class_index_mapping = {0: "Negative",
                               1: "Positive",
                               2: "Neutral",
                               3: "Irrelevant"}

        map_sentiment_udf = udf(lambda index: class_index_mapping.get(
            int(index), "Unknown"), StringType())

        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_brokers) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .load()
        kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as json")
        kafka_df = kafka_df.select(from_json(col("json"), self.schema).alias(
            "data")).select("data.*")

        # Clean text
        clean_udf = self.spark.udf.register("clean_text",
                                            self.clean_text,
                                            StringType())
        kafka_df = kafka_df.withColumn("cleaned_text",
                                       clean_udf(col("comment")))

        # Classification model
        predictions = self.pipeline.transform(kafka_df.withColumnRenamed(
            "cleaned_text", "Text"
        ))

        predictions = predictions.withColumn("SentimentClass",
                                             map_sentiment_udf(
                                                 col("prediction")))

        query = predictions.select("comment", "Text", "SentimentClass") \
            .writeStream \
            .format("memory") \
            .queryName("sentiment_results") \
            .outputMode("append") \
            .start()

        self.run_gradio()

        query.awaitTermination()

    def run_gradio(self):
        def get_results():
            try:
                df = self.spark.sql("SELECT * FROM sentiment_results")
                results = df.collect()
                return [(row["comment"], row["Text"], row["SentimentClass"])
                        for row in results]
            except SystemError:
                return []

        def interface():
            data = get_results()
            return data

        gr.Interface(
            fn=interface,
            inputs=[],
            outputs=gr.DataFrame(headers=["Original Comment",
                                          "Cleaned Text",
                                          "Sentiment"]),
            live=True
        ).launch(share=True)

    def run_in_thread(self):
        thread = Thread(target=self.start_stream)
        thread.start()
