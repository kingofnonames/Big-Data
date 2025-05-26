# # from pyspark.sql import SparkSession
# # from pyspark.sql.functions import col, from_json, explode, split, udf, monotonically_increasing_id
# # from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
# # from pyspark.ml.feature import Tokenizer, HashingTF, IDF
# # import re

# # HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"
# # ES_HOST = "elasticsearch_final"
# # ES_INDEX = "tfidf_index"


# # def run_tfidf_job(input_path, es_host=None, es_index=None):
# #     spark = SparkSession.builder.appName("TFIDF_Spark_Job").getOrCreate()

# #     inner_schema = StructType() \
# #         .add("index_question", IntegerType()) \
# #         .add("question", StringType()) \
# #         .add("url", StringType()) \
# #         .add("tags", ArrayType(StringType())) \
# #         .add("created_date", StringType())

# #     # Hàm làm sạch câu hỏi
# #     def clean_text(text):
# #         if text is None:
# #             return ""
# #         text = re.sub(
# #             r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợ"
# #             r"ùúủũụưừứửữựỳýỷỹỵđÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖ"
# #             r"ỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ\s]", " ", text)
# #         text = text.lower()
# #         text = re.sub(r"\s+", " ", text)
# #         return text.strip()

# #     clean_text_udf = udf(clean_text, StringType())

# #     df_raw = spark.read.json(input_path)
# #     df_parsed = df_raw.withColumn(
# #         "parsed", from_json(col("json_str"), inner_schema))
# #     df_questions = df_parsed.select(col("parsed.question").alias("question"))

# #     df_clean = df_questions.withColumn(
# #         "clean_text", clean_text_udf(col("question")))

# #     df_with_id = df_clean.withColumn("doc_id", monotonically_increasing_id())

# #     tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
# #     df_words = tokenizer.transform(df_with_id)

# #     hashing_tf = HashingTF(
# #         inputCol="words", outputCol="rawFeatures", numFeatures=10000)
# #     featurized_data = hashing_tf.transform(df_words)

# #     idf = IDF(inputCol="rawFeatures", outputCol="features")
# #     idf_model = idf.fit(featurized_data)
# #     rescaled_data = idf_model.transform(featurized_data)

# #     def extract_tfidf(words, features):
# #         from collections import defaultdict
# #         result = []
# #         indices = features.indices
# #         values = features.values
# #         for i, tfidf in zip(indices, values):
# #             result.append((i, tfidf))
# #         return result

# #     from pyspark.sql.types import ArrayType, StructType, StructField, IntegerType, DoubleType

# #     extract_udf = udf(extract_tfidf, ArrayType(StructType([
# #         StructField("index", IntegerType()),
# #         StructField("tfidf", DoubleType())
# #     ])))

# #     df_tfidf = rescaled_data.withColumn(
# #         "tfidf_values", extract_udf(col("words"), col("features")))

# #     df_exploded = df_tfidf.select(
# #         "doc_id", "words", explode("tfidf_values").alias("entry"))
# #     df_word_tfidf = df_exploded.withColumn("index", col(
# #         "entry.index")).withColumn("tfidf", col("entry.tfidf"))

# #     def get_word_at(words, index):
# #         if index < len(words):
# #             return words[index]
# #         return None

# #     get_word_udf = udf(get_word_at, StringType())

# #     df_final = df_word_tfidf.withColumn("word", get_word_udf(col("words"), col(
# #         "index"))).select("word", "tfidf").filter(col("word").isNotNull())

# #     df_grouped = df_final.groupBy("word").avg("tfidf").withColumnRenamed(
# #         "avg(tfidf)", "tfidf").orderBy(col("tfidf").desc())

# #     if es_host and es_index:
# #         df_grouped.write \
# #             .format("org.elasticsearch.spark.sql") \
# #             .option("es.nodes", es_host) \
# #             .option("es.port", "9200") \
# #             .option("es.resource", es_index) \
# #             .mode("overwrite") \
# #             .save()

# #     spark.stop()


# # if __name__ == "__main__":
# #     run_tfidf_job(HDFS_INPUT_PATH, ES_HOST, ES_INDEX)

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, explode, split, udf, monotonically_increasing_id, expr
# from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, DoubleType, StructField
# from pyspark.ml.feature import Tokenizer, HashingTF, IDF
# import re
# import mmh3  # MurmurHash3 (pip install mmh3)
# from pyspark.ml.linalg import SparseVector

# HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"
# ES_HOST = "elasticsearch_final"
# ES_INDEX = "tfidf_index"
# NUM_FEATURES = 10000


# def run_tfidf_job(input_path, es_host=None, es_index=None):
#     spark = SparkSession.builder.appName("TFIDF_Spark_Job").getOrCreate()

#     # Schema cho dữ liệu json
#     inner_schema = StructType() \
#         .add("index_question", IntegerType()) \
#         .add("question", StringType()) \
#         .add("url", StringType()) \
#         .add("tags", ArrayType(StringType())) \
#         .add("created_date", StringType())

#     # Hàm làm sạch câu hỏi
#     def clean_text(text):
#         if text is None:
#             return ""
#         text = re.sub(
#             r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợ"
#             r"ùúủũụưừứửữựỳýỷỹỵđÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖ"
#             r"ỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ\s]", " ", text)
#         text = text.lower()
#         text = re.sub(r"\s+", " ", text)
#         return text.strip()

#     clean_text_udf = udf(clean_text, StringType())

#     # Đọc dữ liệu raw json
#     df_raw = spark.read.json(input_path)
#     df_parsed = df_raw.withColumn(
#         "parsed", from_json(col("json_str"), inner_schema))
#     df_questions = df_parsed.select(col("parsed.question").alias("question"))

#     # Làm sạch text
#     df_clean = df_questions.withColumn(
#         "clean_text", clean_text_udf(col("question")))

#     # Thêm id cho mỗi document
#     df_with_id = df_clean.withColumn("doc_id", monotonically_increasing_id())

#     # Tách từ
#     tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
#     df_words = tokenizer.transform(df_with_id)

#     # HashingTF tính raw features
#     hashing_tf = HashingTF(
#         inputCol="words", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
#     featurized_data = hashing_tf.transform(df_words)

#     # Tính IDF
#     idf = IDF(inputCol="rawFeatures", outputCol="features")
#     idf_model = idf.fit(featurized_data)
#     rescaled_data = idf_model.transform(featurized_data)

#     # UDF tính hash giống HashingTF (dùng murmur3 32bit, seed=42)
#     def hash_term(term):
#         return mmh3.hash(term, 42) % NUM_FEATURES

#     hash_udf = udf(hash_term, IntegerType())

#     # Tách từng từ ra từng dòng riêng
#     df_exploded_words = df_words.select(
#         "doc_id", explode("words").alias("word"))

#     # Tạo cột hash index từ từng từ
#     df_exploded_words = df_exploded_words.withColumn(
#         "hash_index", hash_udf(col("word")))

#     # Tách features vector thành cặp (index, tfidf)

#     def extract_features(v):
#         # v: SparseVector
#         return list(zip(v.indices.tolist(), v.values.tolist()))

#     extract_features_udf = udf(extract_features, ArrayType(StructType([
#         StructField("index", IntegerType()),
#         StructField("tfidf", DoubleType())
#     ])))

#     df_features = rescaled_data.select("doc_id", "features") \
#         .withColumn("tfidf_array", extract_features_udf(col("features"))) \
#         .select("doc_id", explode("tfidf_array").alias("tfidf_entry")) \
#         .select(
#             "doc_id",
#             col("tfidf_entry.index").alias("hash_index"),
#             col("tfidf_entry.tfidf").alias("tfidf")
#     )

#     # Join giữa từ (hash_index) và giá trị tfidf
#     df_join = df_exploded_words.join(
#         df_features, on=["doc_id", "hash_index"], how="inner")

#     # Tính tfidf trung bình cho mỗi từ trên toàn bộ docs
#     df_word_tfidf_avg = df_join.groupBy("word").avg("tfidf").withColumnRenamed("avg(tfidf)", "tfidf") \
#         .orderBy(col("tfidf").desc())

#     # Ghi ra Elasticsearch
#     if es_host and es_index:
#         df_word_tfidf_avg.write \
#             .format("org.elasticsearch.spark.sql") \
#             .option("es.nodes", es_host) \
#             .option("es.port", "9200") \
#             .option("es.resource", es_index) \
#             .mode("overwrite") \
#             .save()

#     spark.stop()


# if __name__ == "__main__":
#     run_tfidf_job(HDFS_INPUT_PATH, ES_HOST, ES_INDEX)


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, udf, monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, DoubleType, StructField
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.linalg import SparseVector
import re
import mmh3
HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"

NUM_FEATURES = 10000

# --- UDFs ---


def clean_text(text):
    if text is None:
        return ""
    text = re.sub(
        r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợ"
        r"ùúủũụưừứửữựỳýỷỹỵđÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖ"
        r"ỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ\s]", " ", text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


clean_text_udf = udf(clean_text, StringType())


def hash_term(term):
    return mmh3.hash(term, 42) % NUM_FEATURES


hash_udf = udf(hash_term, IntegerType())


def extract_features(v):
    # v: SparseVector
    return list(zip(v.indices.tolist(), v.values.tolist()))


extract_features_udf = udf(extract_features, ArrayType(StructType([
    StructField("index", IntegerType()),
    StructField("tfidf", DoubleType())
])))


def run_tfidf_job(input_path, es_host=None, es_index=None):
    spark = SparkSession.builder.appName("TFIDF_Spark_Job").getOrCreate()

    inner_schema = StructType() \
        .add("index_question", IntegerType()) \
        .add("question", StringType()) \
        .add("url", StringType()) \
        .add("tags", ArrayType(StringType())) \
        .add("created_date", StringType())

    df_raw = spark.read.json(input_path)
    df_parsed = df_raw.withColumn(
        "parsed", from_json(col("json_str"), inner_schema))
    df_questions = df_parsed.select(col("parsed.question").alias("question"))

    df_clean = df_questions.withColumn(
        "clean_text", clean_text_udf(col("question")))
    df_with_id = df_clean.withColumn("doc_id", monotonically_increasing_id())

    tokenizer = Tokenizer(inputCol="clean_text", outputCol="words")
    df_words = tokenizer.transform(df_with_id)

    hashing_tf = HashingTF(
        inputCol="words", outputCol="rawFeatures", numFeatures=NUM_FEATURES)
    featurized_data = hashing_tf.transform(df_words)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)

    df_exploded_words = df_words.select(
        "doc_id", explode("words").alias("word"))
    df_exploded_words = df_exploded_words.withColumn(
        "hash_index", hash_udf(col("word")))

    df_features = rescaled_data.select("doc_id", "features") \
        .withColumn("tfidf_array", extract_features_udf(col("features"))) \
        .select("doc_id", explode("tfidf_array").alias("tfidf_entry")) \
        .select("doc_id",
                col("tfidf_entry.index").alias("hash_index"),
                col("tfidf_entry.tfidf").alias("tfidf"))

    df_join = df_exploded_words.join(
        df_features, on=["doc_id", "hash_index"], how="inner")

    df_word_tfidf_avg = df_join.groupBy("word").avg("tfidf").withColumnRenamed("avg(tfidf)", "tfidf") \
        .orderBy(col("tfidf").desc())

    if es_host and es_index:
        df_word_tfidf_avg.write.format("org.elasticsearch.spark.sql") \
            .option("es.nodes", es_host) \
            .option("es.port", "9200") \
            .option("es.resource", es_index) \
            .mode("overwrite") \
            .save()

    spark.stop()


if __name__ == "__main__":
    es_host = "elasticsearch_final"
    es_index = "tfidf_index"
    run_tfidf_job(HDFS_INPUT_PATH, es_host, es_index)
