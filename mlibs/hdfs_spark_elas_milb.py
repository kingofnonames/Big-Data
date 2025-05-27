from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, regexp_replace, lower, trim, udf, from_json
from pyspark.sql.types import StringType, StructType, StructField, ArrayType, IntegerType

MODEL_PATH = "/tmp/kmeans_model"
DATA_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"

ES_INDEX = "questions_clustered"
ES_NODES = "elasticsearch_final"
ES_PORT = "9200"

label_map = {
    0: "Pháp luật đất đai & Giá đất",
    1: "Dự án đầu tư & Chủ đầu tư BĐS",
    2: "Chuyển mục đích & Quyền sử dụng đất",
    3: "Hợp đồng thuê & mua bán nhà ở",
    4: "Kinh doanh BĐS & Pháp lý dự án",
    5: "Phát triển nhà ở & Chính sách hỗ trợ",
    6: "Quy hoạch & Kế hoạch sử dụng đất",
    7: "Chung cư & Tái định cư",
    8: "Thuế – Thế chấp – Tái định cư",
    9: "Giấy chứng nhận & Quyền sở hữu đất",
}


def clean_text_df(df, input_col, output_col):
    pattern = r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ\s]"
    df = df.withColumn(output_col, regexp_replace(
        col(input_col), pattern, " "))
    df = df.withColumn(output_col, lower(col(output_col)))
    df = df.withColumn(output_col, regexp_replace(
        col(output_col), r"\s+", " "))
    df = df.withColumn(output_col, trim(col(output_col)))
    return df


def map_label(prediction):
    return label_map.get(prediction, "Unknown")


map_label_udf = udf(map_label, StringType())


def predict_and_save_to_es():
    spark = SparkSession.builder \
        .appName("PredictAndSaveToES") \
        .config("spark.es.nodes", ES_NODES) \
        .config("spark.es.port", ES_PORT) \
        .getOrCreate()
    # .config("spark.es.nodes.wan.only", "true") \
    inner_schema = StructType([
        StructField("index_question", IntegerType(), True),
        StructField("question", StringType(), True),
        StructField("url", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("created_date", StringType(), True)
    ])

    model = PipelineModel.load(MODEL_PATH)

    df_raw = spark.read.json(DATA_PATH)

    df_parsed = df_raw.withColumn(
        "json_data", from_json(col("json_str"), inner_schema))
    # Làm sạch giống như lúc training
    df = df_parsed.select(col("json_data.question").alias("question")) \
                  .na.drop(subset=["question"])
    df = clean_text_df(df, "question", "clean_question")

    predicted = model.transform(df)

    result = predicted.select("question", "prediction") \
                      .withColumn("label_text", map_label_udf(col("prediction")))

    result.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", ES_INDEX) \
        .mode("overwrite") \
        .save()

    spark.stop()


if __name__ == "__main__":
    predict_and_save_to_es()
