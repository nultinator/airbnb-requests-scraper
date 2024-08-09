import os
import csv
import requests
import json
import logging
from urllib.parse import urlencode
from bs4 import BeautifulSoup
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def find_pagination_urls(keyword, location, pages=4, retries=3):
    formatted_keyword = keyword.replace(", ", "--").replace(" ", "-")
    url = f"https://www.airbnb.com/s/{formatted_keyword}/homes"
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")            

            soup = BeautifulSoup(response.text, "html.parser")
            pagination_bar = soup.select_one("nav[aria-label='Search results pagination']")
            a_tags = pagination_bar.find_all("a")
            links = []
            links.append(url)
            acceptable_pages = ["1", "2", "3", "4"]
            for a in a_tags:
                if a.text in acceptable_pages and len(links) < pages:
                    href = a.get("href")
                    link = f"https://www.airbnb.com{href}"
                    links.append(link)
            success = True
            return links

        except Exception as e:
            logger.warning(f"Failed to fetch page list for {url} tries left {retries - tries}")
            logger.warning(f"Exception: {e}")
            tries += 1
    if not success:
        raise Exception("Failed to find pagination, max retries exceeded!")
    



def scrape_search_results(url, location, retries=3):
    tries = 0
    success = False
    
    while tries <= retries and not success:
        try:
            response = requests.get(url)
            logger.info(f"Recieved [{response.status_code}] from: {url}")
            if response.status_code != 200:
                raise Exception(f"Failed request, Status Code {response.status_code}")
                
            soup = BeautifulSoup(response.text, "html.parser")            
            div_cards = soup.select("div[data-testid='card-container']")

            
            for div_card in div_cards:
                descripition = div_card.select_one("div[data-testid='listing-card-title']").text
                subtitle_array = div_card.select("div[data-testid='listing-card-subtitle']")

                name = subtitle_array[0].text
                dates = subtitle_array[-1].text

                price = div_card.select_one("span div span").text
                href = div_card.find("a").get("href")
                link = f"https://www.airbnb.com{href}"
                
                search_data = {
                    "name": name,
                    "description": descripition,
                    "dates": dates,
                    "price": price,
                    "url": link
                }

                print(search_data)
                
            logger.info(f"Successfully parsed data from: {url}")
            success = True       
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries +=1

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")




def start_scrape(url_list, location, retries=3):
    for url in url_list:
        scrape_search_results(url, location, retries=retries)


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["Myrtle Beach, South Carolina, United States"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(", ", "-").replace(" ", "-")

        page_urls = find_pagination_urls(keyword, LOCATION, pages=PAGES, retries=MAX_RETRIES)
        
        start_scrape(page_urls, LOCATION, retries=MAX_RETRIES)
    logger.info(f"Crawl complete.")