from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType


def read_hdfs_json(spark, path):
    # Đọc tất cả file JSON trong thư mục HDFS
    return spark.read.json(path)


def compute_tfidf(df, input_col="question"):
    # Kiểm tra cột input_col có tồn tại không
    if input_col not in df.columns:
        raise ValueError(f"DataFrame không có cột '{input_col}'")

    tokenizer = Tokenizer(inputCol=input_col, outputCol="words")
    wordsData = tokenizer.transform(df)

    remover = StopWordsRemover(inputCol="words", outputCol="filtered")
    filteredData = remover.transform(wordsData)

    hashingTF = HashingTF(inputCol="filtered",
                          outputCol="rawFeatures", numFeatures=10000)
    featurizedData = hashingTF.transform(filteredData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    vector_to_array_udf = udf(
        lambda v: v.toArray().tolist() if v is not None else [], ArrayType(DoubleType()))
    df_with_array = rescaledData.withColumn(
        "tfidf_features", vector_to_array_udf("features"))

    return df_with_array


def save_to_es(df, index_name, es_nodes="elasticsearch_final", es_port="9200"):
    df.select("question", "tfidf_features") \
      .write \
      .format("org.elasticsearch.spark.sql") \
      .option("es.resource", f"{index_name}/_doc") \
      .option("es.nodes", es_nodes) \
      .option("es.port", es_port) \
      .option("es.nodes.wan.only", "true") \
      .mode("overwrite") \
      .save()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TFIDF-HDFS-to-ES") \
        .config("spark.es.nodes", "elasticsearch_final") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions/"
    df = read_hdfs_json(spark, HDFS_INPUT_PATH)

    print("Schema of data read from HDFS:")
    df.printSchema()

    print("Sample questions:")
    df.select("question").take(5)

    df_tfidf = compute_tfidf(df)

    print("Data after TF-IDF computation:")
    df_tfidf.select("question", "tfidf_features").take(5)

    # Lưu dữ liệu vào Elasticsearch
    # save_to_es(df_tfidf, "questions_tfidf")

    spark.stop()
