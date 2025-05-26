from crawler import crawl_question
from config import CRAWL_URL_TEST, KAFKA_TOPIC, CRAWL_URL, TOPICS
from crawler import get_driver

if __name__ == "__main__":
    print("=== Bắt đầu crawl và gửi Kafka ===")
    driver = get_driver()
    for i in range(271, 379):
        url = CRAWL_URL.format(i)
        print(url)
        crawl_question(driver, url, TOPICS)

    print("=== Kết thúc ===")
    # options = Options()
    # options.add_argument("--start-maximized")  # Open Edge in maximized mode
    # options.add_experimental_option("excludeSwitches", ["enable-automation"])

    # edge_path = r"D:\webtool\edgedriver_win64\msedgedriver.exe"
    # service = Service(EDGE_DRIVER_PATH)

    # driver = webdriver.Edge(service=service, options=options)
