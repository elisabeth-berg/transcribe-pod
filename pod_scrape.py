import os
import re
import time
import boto3
import requests
from bs4 import BeautifulSoup
s3 = boto3.resource('s3')


def full_scrape(bucket_name, prefix='episodes'):
    page = "https://www.podbean.com/podcast-detail/c4kny-38725/Cooking-Issues-Podcast"
    while page:
        episode_urls, page = get_episode_urls(page)
        downloader(episode_urls, bucket_name, prefix)


def get_episode_urls(page):
    response = requests.get(page)
    soup = BeautifulSoup(response.text, 'html.parser')
    download_items = soup.findAll("a", {"class": "download"})
    urls = [item.get("href") for item in download_items]
    pagination = soup.find("div", {"class": "pagination"})
    next_page = pagination.find("li", {"class": "next"})
    if next_page:
        next_page = "https://www.podbean.com" + next_page.find("a").get("href")
    return urls, next_page


def downloader(episode_urls, bucket_name, prefix):
    for url in episode_urls:
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
        episode_name = soup.find("p", {"class": "pod-name"}).text
        filename = re.sub(r"[^\w\s]| ", "", episode_name.strip()) + ".mp3"
        current_files = [f.key for f in s3.Bucket(bucket_name).objects.all()]
        if filename in current_files:
            print("{} has already been uploaded to S3".format(episode_name))
        else:
            mp3_url = soup.find(
                "a", {"class": "download-btn"}).get("href").split("?")[0]
            mp3 = requests.get(mp3_url)
            s3.Object(bucket_name, prefix + '/' +
                      filename).put(Body=mp3.content)
            print("{} uploaded!".format(episode_name))
            time.sleep(2)


if __name__ == "__main__":
    full_scrape("pod-transcription-storage")
