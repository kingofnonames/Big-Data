from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, date_format
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType

HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"


def process_questions_data(hdfs_path: str, es_host: str, es_index: str):
    spark = SparkSession.builder \
        .appName("hdfs_spark_date") \
        .getOrCreate()

    inner_schema = StructType() \
        .add("index_question", IntegerType()) \
        .add("question", StringType()) \
        .add("url", StringType()) \
        .add("tags", ArrayType(StringType())) \
        .add("created_date", StringType())

    df_raw = spark.read.json(hdfs_path)

    df_parsed = df_raw.withColumn(
        "parsed", from_json(col("json_str"), inner_schema)
    )

    df_parsed = df_parsed.select(
        col("parsed.question"),
        col("parsed.url"),
        col("parsed.tags"),
        col("parsed.created_date")
    )

    # ➤ Chỉ tạo các trường year, month, month_year từ created_date
    df_parsed = df_parsed \
        .withColumn("month", date_format(to_timestamp(col("created_date"), "HH:mm | dd/MM/yyyy"), "MM")) \
        .withColumn("year", date_format(to_timestamp(col("created_date"), "HH:mm | dd/MM/yyyy"), "yyyy")) \
        .withColumn("month_year", date_format(to_timestamp(col("created_date"), "HH:mm | dd/MM/yyyy"), "yyyy-MM"))

    # ➤ Ghi dữ liệu sang Elasticsearch (bỏ created_ts)
    df_parsed.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_host) \
        .option("es.port", "9200") \
        .option("es.resource", es_index) \
        .option("es.mapping.id", "url") \
        .mode("overwrite") \
        .save()

    spark.stop()


if __name__ == "__main__":
    es_host = "elasticsearch_final"
    es_index = "date_index"
    process_questions_data(HDFS_INPUT_PATH, es_host, es_index)
