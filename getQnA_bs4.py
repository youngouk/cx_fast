import sqlite3
import asyncio
import aiohttp
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import logging
from typing import List, Dict, Any
import re
from datetime import datetime

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 상수 정의
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# SQLite 데이터베이스 연결 함수
def get_db_connection():
    return sqlite3.connect('naver_kin.db')

# 스크래핑된 타이틀에서 불필요한 공백과 줄바꿈 제거
def clean_title(raw_title: str) -> str:
    return " ".join(raw_title.replace("질문", "").split()).strip()

# 검색 결과 페이지에서 데이터 추출 함수
async def scrape_search_results(session: ClientSession, search_url: str) -> List[Dict[str, Any]]:
    logger.info(f"Accessing search results page: {search_url}")

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

    html = await fetch_with_retry(search_url)
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    items = soup.select('ul.basic1 li')
    logger.info(f"Found {len(items)} items in the search results.")

    search_results = []
    for index, item in enumerate(items, start=1):
        try:
            title_element = item.select_one('dt a._searchListTitleAnchor')
            raw_title = title_element.text
            title = clean_title(raw_title)
            url = title_element['href']
            date_element = item.select_one('dd.txt_inline')
            date = date_element.text.strip()

            logger.info(f"Scraped item {index}: Title: {title}")
            search_results.append({"title": title, "url": url, "date": date})
        except (AttributeError, TypeError) as e:
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

def save_to_database(conn: sqlite3.Connection, data: Dict[str, Any]):
    cursor = conn.cursor()
    try:
        # 중복 URL 확인 로직 추가
        cursor.execute("SELECT 1 FROM kin_data WHERE url = ?", (data['url'],))
        if cursor.fetchone():
            logger.info(f"URL already exists in DB, skipping: {data['url']}")
            return

        logger.info(f"Saving to DB: {data['title']}")
        cursor.execute('''
            INSERT OR REPLACE INTO kin_data 
            (title, url, date, author, views, created_at, description, tags, scraped_at) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data['title'], data['url'], data['date'],
            data.get('author'), data.get('views'), data.get('created_at'),
            data.get('description'), data.get('tags'), data.get('scraped_at')
        ))
        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()

def url_exists(conn: sqlite3.Connection, url: str) -> bool:
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM kin_data WHERE url = ?", (url,))
    return cursor.fetchone() is not None

async def main():
    conn = get_db_connection()

    try:
        search_url = "https://kin.naver.com/search/list.naver?query=%ED%95%80%EB%8B%A4&section=qna&period=1w&dirId=4&sort=date"

        async with aiohttp.ClientSession() as session:
            search_results = await scrape_search_results(session, search_url)

            tasks = []
            for result in search_results:
                if not url_exists(conn, result['url']):
                    tasks.append(scrape_detail_page(session, result['url']))
                else:
                    logger.info(f"URL already exists in DB, skipping: {result['url']}")

            details = await asyncio.gather(*tasks)

        for result, detail in zip(search_results, details):
            if detail:
                detail.pop('title', None)
                data = {**result, **detail}
                save_to_database(conn, data)
                logger.info(f"Saved data for URL: {result['url']}")
            else:
                logger.warning(f"Failed to scrape details for URL: {result['url']}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    asyncio.run(main())