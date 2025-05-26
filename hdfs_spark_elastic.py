from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, udf
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import re
import sys

HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"
HDFS_OUTPUT_PATH = "hdfs://namenode:8020/test/hadoop/outputs/wordcount"


def run_wordcount_job(input_path, es_host=None, es_index=None):
    spark = SparkSession.builder.appName("hdfs_spark_wordcount").getOrCreate()

    inner_schema = StructType() \
        .add("index_question", IntegerType()) \
        .add("question", StringType()) \
        .add("url", StringType()) \
        .add("tags", ArrayType(StringType())) \
        .add("created_date", StringType())

    def clean_text(text):
        if text is None:
            return ""
        text = re.sub(
            r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ\s]", " ", text)
        text = text.lower()
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    clean_text_udf = udf(clean_text, StringType())

    df_raw = spark.read.json(input_path)
    df_parsed = df_raw.withColumn(
        "parsed", from_json(col("json_str"), inner_schema))
    df_questions = df_parsed.select(col("parsed.question").alias("question"))
    df_clean = df_questions.withColumn(
        "clean_question", clean_text_udf(col("question")))
    df_words = df_clean.select(
        explode(split(col("clean_question"), " ")).alias("word"))
    df_filtered = df_words.filter(col("word") != "")
    df_wordcount = df_filtered.groupBy(
        "word").count().orderBy(col("count").desc())

    if es_host and es_index:
        df_wordcount.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", es_host) \
            .option("es.port", "9200") \
            .option("es.resource", es_index) \
            .mode("overwrite") \
            .save()
        # .option("es.nodes.wan.only", "true") \

    spark.stop()


if __name__ == "__main__":
    es_host = "elasticsearch_final"
    es_index = "wordcount_index"

    run_wordcount_job(HDFS_INPUT_PATH, es_host, es_index)
