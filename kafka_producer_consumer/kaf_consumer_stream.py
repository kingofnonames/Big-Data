from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructField, StructType, StringType, ArrayType, IntegerType

# Tạo SparkSession


def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaToElasticsearch") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,"
                "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
        .getOrCreate()


# Schema cho Kafka message
def define_schema():
    return StructType([
        StructField("index_question", IntegerType(), True),
        StructField("question", StringType(), True),
        StructField("url", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("created_date", StringType(), True)
    ])


# Đọc dữ liệu từ Kafka
def read_kafka_stream(spark, kafka_broker, topic, schema):
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    return df_parsed


# Ghi dữ liệu gốc vào Elasticsearch
def write_main_to_elasticsearch(df):
    return df.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch_final") \
        .option("es.port", "9200") \
        .option("es.resource", "test_questions_stream_0") \
        .outputMode("append") \
        .start()


# Đếm số lần xuất hiện của từng tag
def count_tags_and_write(df):
    df_tags = df.select(explode("tags").alias("tag"))
    df_tag_counts = df_tags.groupBy("tag").count()

    return df_tag_counts.writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch_final") \
        .option("es.port", "9200") \
        .option("es.resource", "test_tags_count") \
        .option("es.mapping.id", "tag") \
        .outputMode("update") \
        .trigger(processingTime="5 seconds") \
        .start()


def main():
    spark = create_spark_session()
    schema = define_schema()

    kafka_broker = "kafka:29092"
    topic = "test_question_stream"

    df_parsed = read_kafka_stream(spark, kafka_broker, topic, schema)

    query_main = write_main_to_elasticsearch(df_parsed)
    query_tags = count_tags_and_write(df_parsed)

    query_main.awaitTermination()
    query_tags.awaitTermination()


if __name__ == "__main__":
    main()
