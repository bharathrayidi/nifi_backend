# news.py
import requests
import feedparser
from bs4 import BeautifulSoup
from textwrap import shorten
from typing import List, Dict
import time
from datetime import datetime

class YahooNewsFetcher:
    def __init__(self, limit: int = 20):
        self.rss_url = "https://www.yahoo.com/news/rss/"
        self.headers = {"User-Agent": "Mozilla/5.0"}
        self.limit = limit

    def fetch_article_content(self, url: str):
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")
                title = soup.find("h1").text if soup.find("h1") else "No title"
                time_tag = soup.find("time")
                date = time_tag['datetime'] if time_tag and time_tag.has_attr('datetime') else "No date"
                paragraphs = soup.find_all("p")
                content = " ".join([p.text for p in paragraphs])
                return title, date, content
        except Exception:
            pass
        return None, None, None

    def summarize_content(self, text: str, length: int = 300) -> str:
        return shorten(text, width=length, placeholder="...")

    def parse_date(self, date_str: str):
        """Convert a date string to a datetime object, fallback to None if invalid."""
        try:
            # Try common formats or rely on feedparser's structured time if available
            return datetime.fromisoformat(date_str)
        except Exception:
            try:
                return datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
            except Exception:
                return None

    def get_news(self) -> List[Dict[str, str]]:
        feed = feedparser.parse(self.rss_url)
        news_list = []
        for entry in feed.entries[:self.limit]:
            title, date_str, content = self.fetch_article_content(entry.link)
            if not content:
                title = entry.title
                date_str = getattr(entry, 'published', 'No date')
                content = "Content not available"
            summary = self.summarize_content(content, length=500)
            date_obj = self.parse_date(date_str)
            news_list.append({
                "title": title,
                "link": entry.link,
                "published_date": date_str,
                "summary": summary,
                "_date_obj": date_obj  # temporary field for sorting
            })
            time.sleep(1)  # optional: throttle requests

        # Sort by date_obj, placing None at the end
        news_list.sort(key=lambda x: (x["_date_obj"] is None, x["_date_obj"]), reverse=True)

        # Remove temporary field before returning
        for news in news_list:
            news.pop("_date_obj", None)

        return news_list
