from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, lower, trim
from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType

HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"


def process_tags_count(hdfs_path: str, es_host: str, es_index: str):
    spark = SparkSession.builder \
        .appName("tags_count") \
        .getOrCreate()

    schema = StructType() \
        .add("index_question", IntegerType()) \
        .add("question", StringType()) \
        .add("url", StringType()) \
        .add("tags", ArrayType(StringType())) \
        .add("created_date", StringType())

    # Đọc file JSON thô
    df_raw = spark.read.json(hdfs_path)

    # parse json_str nếu dữ liệu của bạn có field json_str, hoặc bỏ bước này nếu dữ liệu đã flat
    from pyspark.sql.functions import from_json
    df_parsed = df_raw.withColumn("parsed", from_json(col("json_str"), schema)) \
                      .select("parsed.*")

    # Explode tags ra từng tag riêng biệt
    df_tags = df_parsed.select(explode(col("tags")).alias("tag_raw"))

    # Làm sạch tag: trim, lowercase (tùy ý)
    df_tags_clean = df_tags.withColumn("tag", lower(trim(col("tag_raw"))))

    # Đếm số lần xuất hiện mỗi tag
    df_count = df_tags_clean.groupBy(
        "tag").count().orderBy(col("count").desc())

    # Viết ra Elasticsearch
    df_count.write.format("org.elasticsearch.spark.sql") \
        .option("es.nodes", es_host) \
        .option("es.port", "9200") \
        .option("es.resource", es_index) \
        .mode("overwrite") \
        .save()

    spark.stop()


if __name__ == "__main__":
    es_host = "elasticsearch_final"
    es_index = "tags_count_index_hdfs"
    process_tags_count(HDFS_INPUT_PATH, es_host, es_index)
