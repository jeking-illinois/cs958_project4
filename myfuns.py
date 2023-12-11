import pandas as pd
import numpy as np
import requests
import io
np.random.seed(7306)

COLUMN_NAMES_MOVIES = ["movie_id", "name", "genres"]
COLUMN_NAMES_RATINGS = ["user_id", "movie_id", "rating", "timestamp"]
UNIQUE_GENRES = ['Action', 'Adventure', 'Animation', 'Childrens', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Fantasy', 'Film-Noir', 'Horror', 'Musical', 'Mystery', 'Romance', 'Sci-Fi', 'Thriller', 'War', 'Western']
# Define the URL for movie data
#movies_url = "https://liangfgithub.github.io/MovieData/movies.dat?raw=true"
#ratings_url = "https://liangfgithub.github.io/MovieData/ratings.dat?raw=true"
# PROCESSING DONE IN NOTEBOOK & UPLOADED TO AWS S3 WITH S_TOP_30
url = "https://jeking598.s3.us-east-2.amazonaws.com/movies.csv"

# Fetch the data from the URL
r = requests.get(url)
movies = pd.read_csv(io.StringIO(r.text))

genres = UNIQUE_GENRES

def get_displayed_movies():
    m = movies.copy()
    m = m.sample(frac=1)
    return m.head(100)

def get_recommended_movies(rating_input):
    url = "https://jeking598.s3.us-east-2.amazonaws.com/S_top_30.csv"
    r = requests.get(url)
    S = pd.read_csv(io.StringIO(r.text), index_col=0)
    
    w = S.loc["m1"]
    w.name = "new_user"
    w[w > 0] = 0
    w = w.replace(0, np.nan)
    for key, value in rating_input.items():
        key = "m" + str(key)
        w[key] = value
    
    w_mask = w.copy()
    w_mask[w_mask != 0] = 1
    N = S.multiply(w).sum(axis=1)
    D = S.multiply(w_mask).sum(axis=1)
    RANK = N / D
    RANK = RANK.nlargest(10).index.values.tolist()
    RANK = [ele.replace("m", "") for ele in RANK]
    RANK = [int(i) for i in RANK]
    mov = movies[movies["movie_id"].isin(RANK)]

    return mov
       

def get_popular_movies(genre: str):
    m = movies.loc[(movies[genre] == 1) & (movies["rank"] > 0)]
    return m.sort_values(by=["rank"], ascending=True)[0:10]