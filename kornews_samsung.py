import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
import os
from pyairtable import Api
from pyairtable.formulas import match
from datetime import datetime

import sys
print("🐍 Script started once. argv:", sys.argv)


# Airtable configuration
AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
BASE_ID = "app4m1JPbIkIoGYHd"
TABLE_ID = "tblZEuzZNvyoapOLv"

FIELD_IDS = {
    "Title": "fldeICSiWka4SiwCF",
    "ArticleURL": "flda4q2UUI2Kj3Nnd",
    "ImageURL": "fld08fxem8U4UgWnu",
    "Category": "fldxObNcZ9LtcJEAj",
    "Summary": "fldhAsFalreKdeBsQ",
    "PublishedDate": "fldUfT83CSpUeA3ir",
    "UniqueID": "fldLfdJUsZxVuOzjn"
}

api = Api(AIRTABLE_API_KEY)
table = api.table(BASE_ID, TABLE_ID)


def parse_date(mk_date: str) -> str:
    """Convert MK datetime string to ISO date (YYYY-MM-DD)"""
    try:
        dt = datetime.strptime(mk_date.strip(), "%Y.%m.%d %H:%M:%S")
        return dt.date().isoformat()  # e.g. "2025-06-09"
    except Exception:
        return None


def article_exists(unique_id):
    """Check if an article already exists in Airtable using UniqueID (URL)"""
    formula = match({FIELD_IDS["UniqueID"]: unique_id})
    records = table.all(formula=formula)
    return len(records) > 0


def upload_to_airtable(article):
    """Upload one article to Airtable, if not already uploaded"""
    if article_exists(article["id"]):
        print(f"⚠️ Already exists, skipping: {article['id']}")
        return

    record = {
        FIELD_IDS["Title"]: article["title"],
        FIELD_IDS["ArticleURL"]: article["link"],
        FIELD_IDS["ImageURL"]: article["image_url"],
        FIELD_IDS["Category"]: article["category"],
        FIELD_IDS["Summary"]: article["summary"],
        FIELD_IDS["PublishedDate"]: article["published_time"],
        FIELD_IDS["UniqueID"]: article["id"]
    }

    table.create(record)
    print(f"✅ Uploaded: {article['title']}")


def search_mk(keyword: str, max_pages: int = 3):
    base_url = "https://www.mk.co.kr/search"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    results = []

    for page in range(1, max_pages + 1):
        params = {"page": page, "word": keyword}
        encoded_params = urllib.parse.urlencode(params)
        url = f"{base_url}?{encoded_params}"

        print(f"📄 Fetching: {url}")
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print(f"❌ Failed to fetch page {page}")
            continue

        soup = BeautifulSoup(res.text, "html.parser")
        articles = soup.select("li.news_node")

        for article in articles:
            title_tag = article.select_one("h3.news_ttl")
            link_tag = article.find("a")
            image_tag = article.select_one(".thumb_area img")
            category_tag = article.select_one(".cate")
            summary_tag = article.select_one(".news_desc")
            time_tag = article.select_one(".time_info")

            if title_tag and link_tag:
                article_url = link_tag["href"]
                image_url = image_tag.get("data-src") if image_tag else None
                results.append({
                    "id":
                    article_url,
                    "title":
                    title_tag.get_text(strip=True),
                    "link":
                    article_url,
                    "image_url":
                    image_url,
                    "category":
                    category_tag.get_text(
                        strip=True) if category_tag else None,
                    "summary":
                    summary_tag.get_text(strip=True) if summary_tag else None,
                    "published_time":
                    parse_date(time_tag.get_text()) if time_tag else None
                })

        time.sleep(1)  # polite crawling

    return results


if __name__ == "__main__":
    keyword = "삼성전자"
    articles = search_mk(keyword, max_pages=3)
    for article in articles:
        upload_to_airtable(article)

