from crawler import crawl_question
from config import CRAWL_URL_TEST, KAFKA_TOPIC, CRAWL_URL, TOPICS
from crawler import get_driver

if __name__ == "__main__":
    print("=== Bắt đầu crawl và gửi Kafka ===")
    driver = get_driver()
    for i in range(315, 379):
        url = CRAWL_URL.format(i)
        print(url)
        crawl_question(driver, url, TOPICS)

    print("=== Kết thúc ===")
