from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.common.exceptions import NoSuchElementException
import time
from config import EDGE_DRIVER_PATH, CRAWL_URL
from utils import clean_text
from kafka_producer_consumer.kaf_producer import send_to_kafka

index_question = 0


def get_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    service = Service(EDGE_DRIVER_PATH)
    return webdriver.Edge(service=service, options=options)


def crawl_question(driver, url: str, topics: list):
    global index_question
    # driver = get_driver()
    driver.get(url)
    time.sleep(1)

    try:
        articles = driver.find_elements(By.TAG_NAME, "article")
        for article in articles:
            try:
                a_tag = article.find_element(By.TAG_NAME, 'a')
                if not a_tag:
                    continue

                url_answer = a_tag.get_attribute('href')
                question = clean_text(a_tag.get_attribute("title"))

                tags_elem = article.find_element(By.CLASS_NAME, "keyword")
                tag_list = [clean_text(tag.text) for tag in tags_elem.find_elements(
                    By.TAG_NAME, "span")]

                created_date_elems = article.find_elements(
                    By.CLASS_NAME, "sub-time")
                created_date = created_date_elems[0].text if created_date_elems else "N/A"

                question_data = {
                    "index_question": index_question,
                    "question": question,
                    "url": url_answer,
                    "tags": tag_list,
                    "created_date": created_date,
                }
                print(topics)
                for topic in topics:
                    send_to_kafka(topic, question_data)
                    print(f"Sent {topic}")
                index_question += 1

            except NoSuchElementException:
                continue
            except Exception as e:
                print(f"Lỗi xử lý article: {e}")
    except Exception as e:
        print(f"Lỗi truy cập trang {url}: {e}")


if __name__ == "__main__":
    pass
