
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, udf, monotonically_increasing_id
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType, DoubleType, StructField
from pyspark.ml.feature import Tokenizer, CountVectorizer, IDF
from pyspark.ml.linalg import SparseVector
import re

HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"
NUM_FEATURES = 22300

stop_words = set([
    'là', 'có', 'gì', 'ai', 'đâu', 'nào', 'sao', 'thế', 'nào', '?', 'tại', 'bao', 'nhiêu',
    'khi', 'lúc', 'bao giờ', 'thế nào', 'tại sao', 'vì sao', 'làm sao',
    'và', 'với', 'hoặc', 'hay', 'nhưng', 'mà', 'song', 'tuy', 'dù', 'nếu', 'nước',
    'trong', 'ngoài', 'trên', 'dưới', 'sau', 'trước', 'giữa', 'bên', 'cạnh', 'gần',
    'xa', 'từ', 'đến', 'về', 'cho', 'của', 'theo', 'bằng', 'qua', 'khỏi',
    'tôi', 'bạn', 'anh', 'chị', 'em', 'ông', 'bà', 'cô', 'chú', 'cậu', 'mày', 'tao',
    'nó', 'họ', 'chúng', 'mình', 'ta', 'chúng ta', 'chúng tôi', 'người ta',
    'này', 'đó', 'kia', 'ấy', 'đây', 'đấy', 'kìa',
    'các', 'những', 'mọi', 'mỗi', 'toàn', 'cả', 'tất cả', 'hết', 'xong',
    'một', 'hai', 'ba', 'nhiều', 'ít', 'vài', 'mấy', 'đôi',
    'rất', 'lắm', 'hơn', 'nữa', 'thêm', 'bớt', 'kém', 'nhất', 'cùng',
    'cũng', 'còn', 'chỉ', 'mới', 'đã', 'sẽ', 'đang', 'vừa', 'vẫn', 'luôn', 'thường',
    'hay', 'thỉnh thoảng', 'đôi khi', 'thường xuyên', 'liên tục',
    'được', 'bị', 'phải', 'nên', 'cần', 'muốn', 'thích', 'định', 'sắp', 'vừa',
    'có thể', 'không thể', 'chắc chắn', 'có lẽ', 'chắc', 'hẳn',
    'không', 'chẳng', 'chưa', 'đừng', 'thôi',
    'ơi', 'ơ', 'à', 'ạ', 'ý', 'ừ', 'ô', 'ôi', 'ui', 'ối', 'trời',
    'chao', 'dạ', 'vâng', 'ừm', 'hử', 'hả', 'sao', 'gì',
    'để', 'mà', 'cho', 'vì', 'do', 'nên', 'nếu', 'giả sử', 'trong khi', 'khi mà',
    'trước khi', 'sau khi', 'cho đến khi', 'mặc dù', 'dù cho', 'tuy rằng',
    'hôm nay', 'ngày mai', 'hôm qua', 'tuần này', 'tháng này', 'năm nay',
    'bây giờ', 'lúc này', 'hiện tại', 'trước đây', 'sau này', 'tương lai',
    'đây', 'đó', 'kia', 'đâu', 'nơi', 'chỗ', 'vùng', 'vị trí', 'địa điểm',
    'ở', 'tại', 'vào', 'ra', 'lên', 'xuống', 'qua', 'lại',
    'như', 'như thế', 'thật', 'thực', 'quả', 'đúng', 'sai', 'tốt', 'xấu',
    'tuy nhiên', 'tuy vậy', 'do đó', 'vì thế', 'vì vậy', 'bởi vì', 'bởi thế',
    'ngoài ra', 'hơn nữa', 'trước hết', 'đầu tiên', 'cuối cùng', 'sau cùng',
    'thứ nhất', 'thứ hai', 'thứ ba',
    'vs', 'ok', 'à', 'ừm', 'ờ', 'hử', 'ha', 'he', 'hi', 'ho', 'ê', 'ô'
])


def clean_text(text):
    if text is None:
        return ""
    text = re.sub(
        r"[^a-zA-Zàáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệ"
        r"ìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ"
        r"ÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖ"
        r"ỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ\s]", " ", text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    words = text.split()
    filtered = [word for word in words if word not in stop_words]
    return " ".join(filtered).strip()


clean_text_udf = udf(clean_text, StringType())


def extract_features(v):
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
    cv = CountVectorizer(inputCol="words", outputCol="rawFeatures",
                         vocabSize=NUM_FEATURES, minDF=3)
    cv_model = cv.fit(df_words)
    featurized_data = cv_model.transform(df_words)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idf_model = idf.fit(featurized_data)
    rescaled_data = idf_model.transform(featurized_data)
    df_features = rescaled_data.withColumn("tfidf_array", extract_features_udf(col("features"))) \
                               .select("doc_id", explode("tfidf_array").alias("tfidf_entry")) \
                               .select("doc_id",
                                       col("tfidf_entry.index").alias("index"),
                                       col("tfidf_entry.tfidf").alias("tfidf"))
    vocab = cv_model.vocabulary

    def index_to_word(i):
        try:
            return vocab[i]
        except:
            return None
    index_to_word_udf = udf(index_to_word, StringType())
    df_features = df_features.withColumn(
        "word", index_to_word_udf(col("index")))
    df_word_tfidf_avg = df_features.groupBy("word").avg("tfidf") \
        .withColumnRenamed("avg(tfidf)", "avg_tfidf") \
        .orderBy(col("avg_tfidf").desc())
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
