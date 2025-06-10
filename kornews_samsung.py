import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
from pyairtable import Api
from deep_translator import GoogleTranslator
from urllib.parse import urljoin

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
}

api = Api(AIRTABLE_API_KEY)
table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

def get_existing_article_urls():
    """Fetch existing full article URLs to avoid duplicates."""
    existing_urls = set()
    field_name = "Article URL"  # <- use field name here
    print("🔍 Checking for existing articles in Airtable...")
    try:
        records = table.all(fields=[field_name])
        print(f"📦 Total records fetched: {len(records)}")
        for record in records:
            print(f"📄 Record fields: {record['fields']}")
            url = record["fields"].get(field_name)  # <- use field name here too
            if url:
                url_clean = url.strip()
                existing_urls.add(url_clean)
                print(f"✅ Existing URL: {url_clean}")
            else:
                print("⚠️ Missing Article URL in this record.")
    except Exception as e:
        print(f"❌ Error while fetching existing URLs: {e}")
    return existing_urls



def parse_date_from_time_area(time_text: str) -> str:
    """Convert MK time_area text (e.g. 06.09<br/>2025) into YYYY-MM-DD."""
    try:
        parts = time_text.replace("<br/>", " ").split()
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
    base_url = "https://www.mk.co.kr"
    search_url = f"{base_url}/search?word=%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(search_url, headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")
    articles = soup.select("li.news_node")

    existing_urls = get_existing_article_urls()
    print(f"🔁 Loaded {len(existing_urls)} existing article URLs")

    for article in articles:
        title_tag = article.select_one("h3.news_ttl")
        link_tag = article.find("a")
        image_tag = article.select_one(".thumb_area img")
        category_tag = article.select_one(".cate")
        summary_tag = article.select_one(".news_desc")
        time_tag = article.select_one(".time_area span")

        if not (title_tag and link_tag):
            continue

        raw_url = link_tag["href"]
        full_url = urljoin(base_url, raw_url.strip())
        print(f"🔗 Found article URL: {full_url}")

        if full_url in existing_urls:
            print(f"⚠️ Skipping duplicate: {full_url}")
            continue

        title = title_tag.get_text(strip=True)
        category = category_tag.get_text(strip=True) if category_tag else None
        summary = summary_tag.get_text(strip=True) if summary_tag else None
        date_text = time_tag.decode_contents() if time_tag else None
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
            FIELD_MAP["Article URL"]: full_url,
            FIELD_MAP["ImageFile URL"]: image_url,
            FIELD_MAP["Title_Eng"]: title_en,
            FIELD_MAP["Category_Eng"]: category_en,
            FIELD_MAP["Summary_Eng"]: summary_en,
        }

        table.create(fields)
        print(f"✅ Uploaded: {title}")

if __name__ == "__main__":
    scrape_and_upload()
