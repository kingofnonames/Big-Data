import re
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, lower, trim, col, from_json, udf
from pyspark.sql.types import StringType, StructType, ArrayType, DoubleType
from pyspark.ml.feature import Tokenizer, HashingTF
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

# Đường dẫn
LOCAL_MODEL_PATH = "/tmp/kmeans_model_new1"
SPARK_MASTER_OUTPUT = "/tmp/kmeans_clustered_questions_new1"
DATA_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"
LOSS_OUTPUT_PATH = "/tmp/kmeans_loss_report.txt"

# Làm sạch câu hỏi


def clean_text_df(df, input_col, output_col):
    pattern = r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ\s]"
    df = df.withColumn(output_col, regexp_replace(
        col(input_col), pattern, " "))
    df = df.withColumn(output_col, lower(col(output_col)))
    df = df.withColumn(output_col, regexp_replace(
        col(output_col), r"\s+", " "))
    df = df.withColumn(output_col, trim(col(output_col)))
    return df

# Tính khoảng cách Euclidean bình phương từ mỗi điểm đến centroid tương ứng


def compute_distance(features, prediction, centers):
    center = centers[prediction]
    return float(np.linalg.norm(features - center)) ** 2


def train_and_save_kmeans(data_path):
    spark = SparkSession.builder.appName("KMeansModelNew").getOrCreate()
    df_raw = spark.read.json(data_path)

    # Schema nội dung
    inner_schema = StructType() \
        .add("question", StringType()) \
        .add("url", StringType()) \
        .add("tags", ArrayType(StringType())) \
        .add("created_date", StringType())

    df_parsed = df_raw.withColumn(
        "parsed", from_json(col("json_str"), inner_schema))

    df = df_parsed.select(
        col("parsed.question").alias("question"),
        col("parsed.url").alias("url"),
        col("parsed.tags").alias("tags"),
        col("parsed.created_date").alias("created_date")
    ).na.drop(subset=["question"])

    # Làm sạch câu hỏi
    df = clean_text_df(df, "question", "clean_question")

    # Tạo pipeline
    tokenizer = Tokenizer(inputCol="clean_question", outputCol="words")
    hashingTF = HashingTF(
        inputCol="words", outputCol="features", numFeatures=10000)
    kmeans = KMeans(k=10, seed=42)

    pipeline = Pipeline(stages=[tokenizer, hashingTF, kmeans])
    model = pipeline.fit(df)

    # Áp dụng model để dự đoán
    transformed_data = model.transform(df)

    # Lấy model KMeans thực tế từ pipeline
    kmeans_model = model.stages[-1]
    centers = kmeans_model.clusterCenters()

    # Tạo hàm UDF để tính squared error
    def dist_udf(features, prediction):
        return compute_distance(features, prediction, centers)
    squared_error_udf = udf(dist_udf, DoubleType())

    transformed_data = transformed_data.withColumn(
        "squared_error", squared_error_udf(col("features"), col("prediction"))
    )

    # Tổng WSSSE
    loss = transformed_data.agg({"squared_error": "sum"}).collect()[0][0]

    # Ghi loss ra file
    with open(LOSS_OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(f"KMeans loss (WSSSE): {loss}\n")

    # Lưu model
    model.write().overwrite().save(LOCAL_MODEL_PATH)

    # Xuất dữ liệu đã phân cụm
    clustered = transformed_data.select("question", "prediction")
    clustered.write.partitionBy("prediction").mode(
        "overwrite").json(SPARK_MASTER_OUTPUT)

    spark.stop()


if __name__ == "__main__":
    train_and_save_kmeans(DATA_PATH)
