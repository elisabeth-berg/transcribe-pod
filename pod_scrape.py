
import os
import requests
import time
from bs4 import BeautifulSoup


def full_scrape(episode_directory):
    """
    Download every episode of Cooking Issues!
    Starts from the homepage given below, and follows the pagination until there
    are no further pages remaining.

    Params:
    ------
    episode_directory : string, full or relative directory where the episodes
        will be stored

    Return:
    ------
    None (episodes will be present as mp3 files in the given directory)
    """
    page = "https://www.podbean.com/podcast-detail/c4kny-38725/Cooking-Issues-Podcast"
    while page:
        episode_urls, page = get_episode_urls(page)
        downloader(episode_urls, episode_directory)

def get_episode_urls(page):
    """
    Each page will have a list of episodes, each of which has its own download
    page. This function will obtain the list of episode URLs, as well as the
    URL to the next page.

    Params:
    ------
    page : string, the full URL of the page to be scraped for episodes

    Return:
    ------
    urls : list of strings, the URLs for episodes on this page
    next_page : string, the URL for the next page of episodes
    """
    response = requests.get(page)
    soup = BeautifulSoup(response.text,'html.parser')
    download_items = soup.findAll("a", {"class":"download"})
    urls = [item.get("href") for item in download_items]
    pagination = soup.find("div", {"class":"pagination"})
    next_page = pagination.find("li", {"class":"next"})
    if next_page:
        next_page = "https://www.podbean.com" + next_page.find("a").get("href")
    return urls, next_page

def downloader(episode_urls, episode_directory):
    """
    Loop through a list of URLs and obtain the MP3 file from the download link
    on each page. Each MP3 will be stored locally unless it has already been
    downloaded, in which case it will be skipped.

    Params:
    -------
    episode_urls : list of strings, the URLs for episodes. Each should have a
        download button pointing to the MP3 file
    episode_directory : string, full or relative directory where the episodes
        will be stored
    """
    for url in episode_urls:
        soup = BeautifulSoup(requests.get(url).text, 'html.parser')
        episode_name = soup.find("p", {"class":"pod-name"}).text
        filename = episode_name.strip().replace("/", "") + ".mp3"
        if filename in os.listdir(episode_directory):
            print("{} has already been downloaded".format(episode_name))
        else:
            mp3_url = soup.find("a", {"class":"download-btn"}).get("href").split("?")[0]
            mp3 = requests.get(mp3_url)
            with open(episode_directory + filename, "wb") as f:
                f.write(mp3.content)
                print("{} downloaded!".format(episode_name))
            time.sleep(2)

if __name__ == "__main__":
    full_scrape("./episodes/")
