import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
import os
from datetime import datetime
from pyairtable import Api
from pyairtable.formulas import match
from deep_translator import GoogleTranslator

# Airtable configuration
AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = "app4m1JPbIkIoGYHd"
AIRTABLE_TABLE_ID = "tblZEuzZNvyoapOLv"

FIELD_MAP = {
    "Title": "fldeICSiWka4SiwCF",
    "Category": "fldxObNcZ9LtcJEAj",
    "Summary": "fldhAsFalreKdeBsQ",
    "Date": "fldUfT83CSpUeA3ir",
    "Article URL": "flda4q2UUI2Kj3Nnd",
    "ImageFile URL": "fld08fxem8U4UgWnu",
    "Title_Eng": "fldRfFYXbGmSAgqfX",
    "Category_Eng": "fldl2oGg6rB4CKHBr",
    "Summary_Eng": "fldbBvQK2vtMCeMAm",
    "UniqueID": "fldLfdJUsZxVuOzjn"
}

api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

def get_existing_unique_ids():
    """Fetch UniqueID values already in Airtable to avoid duplicates."""
    existing = set()
    field_id = FIELD_MAP["UniqueID"]
    for record in table.all(fields=[field_id]):
        val = record["fields"].get(field_id)
        if val:
            existing.add(val)
    return existing

def parse_date_from_time_area(time_text: str) -> str:
    """Convert MK time_area text (e.g. 06.09<br>2025) into YYYY-MM-DD."""
    try:
        parts = time_text.replace("<br>", " ").split()
        if len(parts) == 2:
            mmdd, yyyy = parts
            date_str = f"{yyyy}-{mmdd.replace('.', '-')}"
            return datetime.strptime(date_str, "%Y-%m-%d").date().isoformat()
    except Exception as e:
        print("❌ Failed to parse date:", e)
    return None

def translate_text(text):
    try:
        return GoogleTranslator(source='ko', target='en').translate(text)
    except Exception as e:
        print(f"⚠️ Translation failed: {e}")
        return None

def scrape_and_upload():
    url = "https://www.mk.co.kr/search?word=%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    articles = soup.select("li.news_node")

    existing_ids = get_existing_unique_ids()
    print(f"🔁 Loaded {len(existing_ids)} existing unique IDs")

    for article in articles:
        title_tag = article.select_one("h3.news_ttl")
        link_tag = article.find("a")
        image_tag = article.select_one(".thumb_area img")
        category_tag = article.select_one(".cate")
        summary_tag = article.select_one(".news_desc")
        time_tag = article.select_one(".time_area span")

        if not (title_tag and link_tag):
            continue

        article_url = link_tag["href"]
        if article_url in existing_ids:
            print(f"⚠️ Skipping duplicate: {article_url}")
            continue

        title = title_tag.get_text(strip=True)
        category = category_tag.get_text(strip=True) if category_tag else None
        summary = summary_tag.get_text(strip=True) if summary_tag else None
        date_text = str(time_tag).split(">")[1].split("<")[0] + " " + str(time_tag).split(">")[2].split("<")[0] if time_tag else None
        published_date = parse_date_from_time_area(date_text) if date_text else None
        image_url = image_tag.get("data-src") if image_tag else None

        # Translate
        title_en = translate_text(title)
        category_en = translate_text(category) if category else None
        summary_en = translate_text(summary) if summary else None

        fields = {
            FIELD_MAP["Title"]: title,
            FIELD_MAP["Category"]: category,
            FIELD_MAP["Summary"]: summary,
            FIELD_MAP["Date"]: published_date,
            FIELD_MAP["Article URL"]: article_url,
            FIELD_MAP["ImageFile URL"]: image_url,
            FIELD_MAP["UniqueID"]: article_url,
            FIELD_MAP["Title_Eng"]: title_en,
            FIELD_MAP["Category_Eng"]: category_en,
            FIELD_MAP["Summary_Eng"]: summary_en,
        }

        table.create(fields)
        print(f"✅ Uploaded: {title}")

if __name__ == "__main__":
    scrape_and_upload()
