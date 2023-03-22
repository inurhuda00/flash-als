import pandas as pd


def getIds():

    linksdf = pd.read_csv('data/links.csv', index_col='movieId',
                          dtype={'imdbId': str, 'tmdbId': str})
    moviesdf = pd.read_csv('data/movies.csv', index_col='movieId')
    # merge movie and link dataframe in columns
    df = pd.concat([moviesdf, linksdf], axis='columns')

    PER_GENRES = 48

    # {"genre": ['imdbId]}
    movieIds = {}
    # convert list
    movieGenres = df['genres'].tolist()

    genres = []

    for i in range(len(movieGenres)):
        genre = movieGenres[i].split('|')[0]
        if genre in movieIds:
            if len(movieIds[genre]) < PER_GENRES:
                movieIds[genre].append(df.iloc[i]['imdbId'])
        else:
            genres.append(genre)
            movieIds[genre] = [df.iloc[i]['imdbId']]

    del movieIds['(no genres listed)']
    return movieIds


if __name__ == "__main__":
    movieIds = getIds()
    print()
