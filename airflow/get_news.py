from bs4 import BeautifulSoup
import requests
import pandas as pd


class NewsExtractor:

    def fetch_news(self, url):
        """Fetch news headlines and category from the given URL."""
        response = requests.get(url, verify=False)
        soup = BeautifulSoup(response.text, "html.parser")
        category = soup.find("h1", attrs={"class": "sectionname"}).text
        news_items = soup.find_all("div", attrs={"class": "ListingNewsWithMEDImage"})
        return category, news_items

    def write_news_to_csv(self, file_path, news_data):
        """Write news headlines and categories to a CSV file."""
        df = pd.DataFrame(news_data, columns=["News", "Category"])
        # Save the DataFrame to a CSV file
        df.to_csv(file_path, index=False, encoding="utf-8")
        print(f"News data saved to {file_path}")

    def extract_headlines(self, news_items, category):
        """Extract headlines from news items."""
        headlines = []
        for item in news_items:
            h3 = item.find("h3")
            if h3:  # Ensure h3 exists
                headlines.append((h3.text.strip(), category))
        return headlines
    
    def run_news_etl(self):
        urls = [
        "https://www.dailythanthi.com/news/india",
        "https://www.dailythanthi.com/news/world",
        "https://www.dailythanthi.com/sports",
        "https://www.dailythanthi.com/news/weather",
        "https://www.dailythanthi.com/cinema"
        ]
        
        all_news = []

    # Fetch and process news for each URL
        for url in urls:
            category, news_items = self.fetch_news(url)  # No need to pass category_class and news_class
            all_news.extend(self.extract_headlines(news_items, category))

    # Write all news to CSV
        #self.write_news_to_csv(r"airflow\news_classifier\tamil_news.csv", all_news)
