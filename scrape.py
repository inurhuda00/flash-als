from bs4 import BeautifulSoup
import requests
from json import dumps


def scrapeMovie(url):
    # get the imdbID
    imdbId = url.split('/')[-2][2:]
    # request to main webpage
    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    source = response.text
    soup = BeautifulSoup(source, 'lxml')

    wrapper = soup.find('div', class_='sc-80d4314-0')
    title = wrapper.h1.text
    year = wrapper.select("a.ipc-link")[0].text

    time = wrapper.select("div > ul > li")[-1].text

    genres = soup.find(
        'div', class_="ipc-chip-list__scroller").findAll('span', class_='ipc-chip__text')

    for i in range(len(genres)):
        genres[i] = str(genres[i].text)

    poster = soup.find(
        'div', class_='ipc-media').select("img")[0].attrs['srcset'].split(" ")[-2:-1][0]

    rating = wrapper.select(
        "div.sc-db8c1937-0.sc-80d4314-3 > div > div:nth-child(1) > a > div > div > div.sc-7ab21ed2-0 > div.sc-7ab21ed2-2 > span.sc-7ab21ed2-1")[0].text

    # request to plot webpage
    response = requests.get(url+'plotsummary')
    source = response.text
    soup = BeautifulSoup(source, 'lxml')

    summary = soup.find('ul', id='plot-summaries-content').li.p.text

    # Create and return dictionary
    json = {'imdbId': imdbId, 'title': title, 'year': year, 'poster': poster,
            'rating': rating, 'summary': summary, 'time': time, 'genres': genres}

    arr = [imdbId, title, year, poster, rating, summary, time, genres]

    # dumps and loads for save
    return dumps(json)


if __name__ == "__main__":
    print(scrapeMovie("https://www.imdb.com/title/tt0073812/"))
