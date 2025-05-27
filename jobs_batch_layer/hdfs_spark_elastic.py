from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, udf
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
import re

# Đường dẫn HDFS
HDFS_INPUT_PATH = "hdfs://namenode:8020/test/hadoop/raw_data/questions_final"

# Elasticsearch config
ES_HOST = "elasticsearch_final"
ES_INDEX = "wordcount_index"

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("hdfs_spark_wordcount").getOrCreate()

# Schema cho dữ liệu JSON
inner_schema = StructType() \
    .add("index_question", IntegerType()) \
    .add("question", StringType()) \
    .add("url", StringType()) \
    .add("tags", ArrayType(StringType())) \
    .add("created_date", StringType())

# Danh sách stop words
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

# UDF làm sạch văn bản


def clean_text(text):
    if text is None:
        return ""
    text = re.sub(r"[^a-zA-Z0-9àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ\s]", " ", text)
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


clean_text_udf = udf(clean_text, StringType())

# UDF kiểm tra từ có nằm trong stop words hay không


def is_not_stop_word(word):
    return word not in stop_words


is_not_stop_word_udf = udf(is_not_stop_word, StringType())

# Đọc và xử lý dữ liệu
df_raw = spark.read.json(HDFS_INPUT_PATH)

df_parsed = df_raw.withColumn(
    "parsed", from_json(col("json_str"), inner_schema))
df_questions = df_parsed.select(col("parsed.question").alias("question"))

df_clean = df_questions.withColumn(
    "clean_question", clean_text_udf(col("question")))
df_words = df_clean.select(
    explode(split(col("clean_question"), " ")).alias("word"))
df_filtered = df_words.filter(
    (col("word") != "") & (~col("word").isin(stop_words)))

df_wordcount = df_filtered.groupBy("word").count().orderBy(col("count").desc())

# Ghi lên Elasticsearch
df_wordcount.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", ES_HOST) \
    .option("es.port", "9200") \
    .option("es.resource", ES_INDEX) \
    .mode("overwrite") \
    .save()

# Kết thúc Spark
spark.stop()
