from selection import getIds
from scrape import scrapeMovie
from tqdm import tqdm
from json import dump, load, loads


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def write_json(new_data, filename='movies.json'):
    with open(filename, 'r+') as file:
        # First we load existing data into a dict.
        movies_list = load(file)
        # append new data with loads json
        movies_list.append(loads(new_data))
        # Sets file's current position at offset.
        file.seek(0)
        # convert back to json.
        dump(movies_list, file, indent=2)


if __name__ == "__main__":
    movieIds = getIds()

    for genre in movieIds.keys():
        imdbIds = movieIds[genre]
        for imdbId in tqdm(imdbIds):
            url = "https://www.imdb.com/title/tt" + imdbId + '/'
            try:
                data = scrapeMovie(url)
                # function to add to JSON
                write_json(data)
            except:
                logger.info(f"error in : {imdbId}")
                pass
