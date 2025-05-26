from pyspark.sql import SparkSession
from pyspark.sql.functions import col

HDFS_OUTPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"
HDFS_CHECKPOINT_PATH = "hdfs://namenode:8020/test/hadoop/checkpoints/kafka_questions_final"

spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
    .getOrCreate()

kafka_broker = "kafka:29092"

TOPICS = "test_question_topic"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", TOPICS) \
    .option("startingOffsets", "latest") \
    .load()


json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

query = json_df.writeStream \
    .format("json") \
    .option("path", HDFS_OUTPUT_PATH) \
    .option("checkpointLocation", HDFS_CHECKPOINT_PATH) \
    .outputMode("append") \
    .start()

query.awaitTermination()
