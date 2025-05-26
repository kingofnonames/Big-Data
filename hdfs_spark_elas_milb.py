from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, regexp_replace, lower, trim

MODEL_PATH = "/tmp/kmeans_model"
NEW_DATA_PATH = "hdfs://namenode:8020/test/hadoop/new_data/full_questions.json"

ES_INDEX = "questions_clustered"
ES_NODES = "elasticsearch_final"
ES_PORT = "9200"


def clean_text_df(df, input_col, output_col):
    pattern = r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ\s]"
    df = df.withColumn(output_col, regexp_replace(
        col(input_col), pattern, " "))
    df = df.withColumn(output_col, lower(col(output_col)))
    df = df.withColumn(output_col, regexp_replace(
        col(output_col), r"\s+", " "))
    df = df.withColumn(output_col, trim(col(output_col)))
    return df


def predict_and_save_to_es():
    spark = SparkSession.builder \
        .appName("PredictAndSaveToES") \
        .config("spark.es.nodes", ES_NODES) \
        .config("spark.es.port", ES_PORT) \
        .config("spark.es.nodes.wan.only", "true") \
        .getOrCreate()

    model = PipelineModel.load(MODEL_PATH)

    df = spark.read.json(NEW_DATA_PATH).na.drop(subset=["question"])

    # Làm sạch giống như lúc training
    df = clean_text_df(df, "question", "clean_question")

    predicted = model.transform(df)

    result = predicted.select("question", "prediction")

    result.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.resource", ES_INDEX) \
        .mode("overwrite") \
        .save()

    spark.stop()


if __name__ == "__main__":
    predict_and_save_to_es()
