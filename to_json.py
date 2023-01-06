from selection import getIds
from scrape import scrapeMovie
from tqdm import tqdm
import pickle
from json import dump, load, loads


import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


REGISTRY = None


def write_json(new_data, filename='data.json'):
    with open(filename, 'r+') as file:
        # First we load existing data into a dict.
        file_data = load(file)
        # Join new_data with file_data inside emp_details
        file_data.append(loads(new_data))
        # Sets file's current position at offset.
        file.seek(0)
        # convert back to json.
        dump(file_data, file, indent=2)


def main(start=0):
    global REGISTRY

    a = start
    movieIds = getIds()

    # populating the database
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
            a += 1

            REGISTRY = pickle.dumps(a)


if __name__ == "__main__":

    start = pickle.loads(REGISTRY) if REGISTRY else 0

    try:
        main(start=start)
    except:
        pass
