import boto3
import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import StaleElementReferenceException, NoSuchElementException
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime
import logging
from typing import List, Dict, Any
import re


# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 상수 정의
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# DynamoDB 테이블 가져오기
def get_dynamodb_table():
    dynamodb = boto3.resource('dynamodb')
    return dynamodb.Table('kin_data')

# Chrome 드라이버 초기화 함수
def initialize_webdriver():
    logger.info("Initializing WebDriver...")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# 스크래핑된 타이틀에서 불필요한 공백과 줄바꿈 제거
def clean_title(raw_title: str) -> str:
    return " ".join(raw_title.replace("질문", "").split()).strip()

# 검색 결과 스크래핑 함수
async def scrape_search_results(driver, search_url: str) -> List[Dict[str, Any]]:
    logger.info(f"Accessing search results page: {search_url}")
    driver.get(search_url)

    # 페이지 로딩을 기다립니다.
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'a._searchListTitleAnchor'))
        )
    except TimeoutException:
        logger.error("Timed out waiting for page to load")
        return []

    items = []
    retries = 0
    while retries < MAX_RETRIES:
        try:
            result_list = driver.find_element(By.CLASS_NAME, 'basic1')
            items = result_list.find_elements(By.TAG_NAME, 'li')
            logger.info(f"Found {len(items)} items in the search results.")
            break
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logger.warning(f"Error encountered while scraping search results: {e}. Retrying...")
            retries += 1
            if retries == MAX_RETRIES:
                logger.error("Max retries reached. Unable to scrape search results.")
                return []
            time.sleep(RETRY_DELAY)

    search_results = []
    for index, item in enumerate(items, start=1):
        try:
            # 타이틀을 추출하고 클린업 처리
            title_element = item.find_element(By.CSS_SELECTOR, 'dt a._searchListTitleAnchor')
            raw_title = title_element.text
            title = clean_title(raw_title)
            url = title_element.get_attribute('href')
            date_element = item.find_element(By.CSS_SELECTOR, 'dd.txt_inline')
            date = date_element.text.strip()

            logger.info(f"Scraped item {index}: Title: {title}")
            search_results.append({"title": title, "url": url, "date": date})
        except (StaleElementReferenceException, NoSuchElementException) as e:
            logger.warning(f"Error scraping item {index}: {e}")

    return search_results

# 상세 정보 스크래핑 함수
async def scrape_detail_page(session: ClientSession, url: str) -> Dict[str, Any]:
    async def fetch_with_retry(url: str) -> str:
        for _ in range(MAX_RETRIES):
            try:
                async with session.get(url) as response:
                    return await response.text()
            except aiohttp.ClientError as e:
                logger.warning(f"Network error while fetching {url}: {e}. Retrying...")
                await asyncio.sleep(RETRY_DELAY)
        logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
        return ""

    html = await fetch_with_retry(url)
    if not html:
        return {}

    soup = BeautifulSoup(html, 'html.parser')

    try:
        detail_title = soup.select_one('.endTitleSection').text.strip()
        user_info = soup.select_one('.userInfo__bullet').text.strip()

        views_element = soup.select_one('.userInfo__bullet .infoItem:nth-of-type(2)')
        created_at_element = soup.select_one('.userInfo__bullet .infoItem:nth-of-type(3)')

        views = int(re.search(r'\d+', views_element.text).group()) if views_element else 0
        created_at = created_at_element.text.replace("작성일", "").strip() if created_at_element else ""

        description = soup.select_one('.questionDetail').text.strip()
        tags = ', '.join([tag.text for tag in soup.select('.tagList a')])

        return {
            "title": detail_title,
            "author": user_info,
            "views": views,
            "created_at": created_at,
            "description": description,
            "tags": tags,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except AttributeError as e:
        logger.error(f"Error parsing detail page {url}: {e}")
        return {}

def save_to_dynamodb(table, data: Dict[str, Any]):
    try:
        logger.info(f"Saving to DynamoDB: {data['title']}")
        table.put_item(Item={
            'url': data['url'],
            'date': data['date'],
            'title': data['title'],
            'author': data.get('author'),
            'views': data.get('views'),
            'created_at': data.get('created_at'),
            'description': data.get('description'),
            'tags': data.get('tags'),
            'scraped_at': data.get('scraped_at')
        })
    except Exception as e:
        logger.error(f"Error saving to DynamoDB: {e}")

def url_exists(table, url: str) -> bool:
    try:
        response = table.get_item(
            Key={
                'url': url,
                'date': 'some_date'  # Sort Key가 있다면 필요
            }
        )
        return 'Item' in response
    except Exception as e:
        logger.error(f"Error checking URL existence in DynamoDB: {e}")
        return False

async def main():
    table = get_dynamodb_table()
    driver = initialize_webdriver()

    try:
        search_url = "https://kin.naver.com/search/list.naver?query=%ED%95%80%EB%8B%A4&section=qna&period=1w&dirId=4&sort=date"
        search_results = await scrape_search_results(driver, search_url)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for result in search_results:
                if not url_exists(table, result['url']):
                    tasks.append(scrape_detail_page(session, result['url']))
                else:
                    logger.info(f"URL already exists in DynamoDB, skipping: {result['url']}")

            details = await asyncio.gather(*tasks)

        for result, detail in zip(search_results, details):
            if detail:
                detail.pop('title', None)
                data = {**result, **detail}
                save_to_dynamodb(table, data)
                logger.info(f"Saved data for URL: {result['url']}")
            else:
                logger.warning(f"Failed to scrape details for URL: {result['url']}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    asyncio.run(main())