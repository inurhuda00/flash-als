from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, g

import pymongo
import json
from random import randint
import re

from flask_bcrypt import Bcrypt
from flask_wtf import FlaskForm
from wtforms import EmailField, PasswordField, SearchField
from wtforms.validators import DataRequired
import time

from flask_paginate import Pagination, get_page_args

from als import RecommendationALS


class UserForm(FlaskForm):
    email = EmailField('email', validators=[DataRequired()])
    password = PasswordField('password', validators=[DataRequired()])


class SearchForm(FlaskForm):
    search = SearchField('search')


# mongodb config
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client['imdb']
moviesCollection = db["movies"]
usersCollection = db["users"]
ratingsCollection = db["ratings"]

# global recEngine
# recEngine = RecommendationALS()

app = Flask(__name__)
app.config.from_object('config')

# bcrypt
bcrypt = Bcrypt(app)

# decoration


def prevent_login_signup(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get('logged_in'):
            flash("You are logged in already")
            return redirect(url_for('home'))
        return fn(*args, **kwargs)
    return wrapper


def required_login(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get('logged_in'):
            flash("You've to login first")
            return redirect(url_for('join'))
        return fn(*args, **kwargs)
    return wrapper


def required_rating(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if session.get('logged_in'):
            if len(g.stars) < 5:
                flash(
                    f"Berikan {5 - len(g.stars)} film rating, untuk lanjut!")
                return redirect(url_for("ask"))
        return fn(*args, **kwargs)
    return wrapper

# end decoration


def convert(input):
    # Converts unicode to string
    if isinstance(input, dict):
        return {convert(key): convert(value) for key, value in input.iteritems()}
    elif isinstance(input, list):
        return [convert(element) for element in input]
    elif isinstance(input, str):
        return input.encode('utf-8')
    else:
        return input


def random_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


def convertStars(stars):
    new_ratings = []
    if stars is not None:
        for star in stars:
            user_ratings = (star.get("userId"), int(
                star.get('movieId')), float(star.get('rating')))
            # inset user id
            new_ratings.append(user_ratings)
    return new_ratings


@app.before_request
def current_user():
    if session.get('logged_in'):
        g.user = usersCollection.find_one(
            {'email': session['logged_in']})
    else:
        g.user = {"email": "Guest"}


@app.before_request
def current_ratings():
    if session.get('logged_in'):
        # get ratings from database
        stars = ratingsCollection.find(
            {"userId": g.user["userId"]}, {'_id': 0})
        g.stars = list(stars)


@app.route('/')
@required_rating
def home():
    movies = moviesCollection.find().limit(8)
    recs = moviesCollection.find().limit(8)

    ratings_added = session.get('add_ratings')
    print(ratings_added)
    if ratings_added is None:
        ratings_added = False

    train = ratings_added if not ratings_added else ratings_added
    print(train)

    return render_template('index.html', recs=list(recs), movies=list(movies), train=train)


@app.route('/join', methods=['GET', 'POST'])
@prevent_login_signup
def join():
    form = UserForm(request.form)

    if request.method == "POST" and form.validate():
        email = request.form['email']
        attempt_password = request.form['password']

        user = usersCollection.find_one({'email': email})

        if user is None:
            usersCollection.insert_one({'userId': random_digits(7), 'email': email, 'password': bcrypt.generate_password_hash(
                attempt_password).decode('UTF-8')})
            session['logged_in'] = email
            flash("You're registerd!. Berikan 5 film rating, untuk lanjut!")
            return redirect(url_for('ask'))
        else:
            if bcrypt.check_password_hash(user['password'], attempt_password):
                session['logged_in'] = user['email']
                flash("logged in!")
                return redirect(url_for('home'))

            else:
                flash("Invalid credentials, Try again!")

    return render_template('join.html', form=form)


@app.route('/logout')
@required_login
def logout():
    session.pop('logged_in', None)
    flash('You have been signed out.')
    return redirect(url_for('home'))


# @app.route('/recomendations')
# @required_login
# def recomendations():
#     if len(g.stars) < 5:
#         flash("Provide some ratings, min 5 items ")
#         return redirect(url_for('movies'))

#     stars = convertStars(g.stars)

#     recEngine.add_ratings(ratings=stars)
#     recEngine.get_sparsity()

#     movies, user_ratings = recEngine.get_recs_users(user_id=g.user['userId'])

#     # movies = moviesCollection.find({'movieId': {'$in': movieIds}})

#     return render_template('recomendations.html', movies=list(movies), user_ratings=list(user_ratings))

@app.route('/movies', defaults={"page": 1})
@app.route('/movies/<page>')
@required_login
@required_rating
def movies(page, genre=None, q=None):

    form = SearchForm(request.form)
    args = request.args.copy()

    search = args.get("search")
    genre = args.get("genre")

    if search is not None:
        search = re.sub("\W+", "", search, 0, re.IGNORECASE)

    print(search, genre)

    page, per_page, offset = get_page_args(per_page=24)

    if genre is not None:
        query = {"genres": genre}

        movies = moviesCollection.find(query)\
            .skip(offset)\
            .limit(per_page)
        movies_count = moviesCollection.count_documents(query)
    elif search is not None:
        query = {"title": {"$regex": f"{str(search)}", "$options": 'i'}}

        movies = moviesCollection.find(query)\
            .skip(offset)\
            .limit(per_page)
        movies_count = moviesCollection.count_documents(query)
    elif genre is not None and search is not None:
        query = {
            "title": {"$regex": f"{str(search)}", "$options": 'i'}, "genres": genre}

        movies = moviesCollection.find(query)\
            .skip(offset)\
            .limit(per_page)
        movies_count = moviesCollection.count_documents(query)
    else:
        movies = moviesCollection.find({}).skip(offset).limit(per_page)
        movies_count = moviesCollection.count_documents({})

    pagination = Pagination(page=page,
                            per_page=per_page,
                            total=movies_count,
                            record_name='movies')

    genres = ['comedy', 'action', 'drama', 'animation', 'documentary', 'sci-fi', 'adventure', 'crime', 'horror', 'children',
              'uncategories', 'fantasy', 'thriller', 'mystery', 'romance', 'western', 'musical', 'war', 'film-noir']
    dict_genres = {}

    for genre in genres:
        args['genre'] = genre
        url = url_for(request.endpoint, **args)
        dict_genres[genre] = url

    return render_template('movies/index.html', form=form, movies=list(movies), page=page, genres=dict_genres, pagination=pagination)


@app.route('/rating/add', methods=['POST'])
@required_login
def add_rating():
    movieId = request.get_json()['movieId']
    rating = request.get_json()['rating']

    exist = ratingsCollection.find_one(
        {"movieId": movieId, "userId": g.user['userId']})

    if exist is None:
        ratingsCollection.insert_one(
            {"userId": g.user['userId'], "movieId": movieId, "rating": rating}
        )
        session['add_ratings'] = True
    else:
        ratingsCollection.update_one({"movieId": movieId}, {
            "$set": {"rating": rating}})

    g.stars = g.stars.append(
        {"userId": g.user['userId'], "movieId": movieId, "rating": rating})

    return jsonify({"status": "ok"})


@app.route('/rating/update', methods=['POST'])
@required_login
def update_rating():
    movieId = request.get_json()['movieId']
    rating = request.get_json()['rating']

    ratingsCollection.update_one({"movieId": movieId}, {
                                 "$set": {"rating": rating}})

    return jsonify({"status": "ok"})


@app.route('/ask', defaults={"page": 1})
@app.route('/ask/<page>')
@required_login
def ask(page, genre=None, q=None):

    form = SearchForm(request.form)
    args = request.args.copy()

    search = args.get("search")
    genre = args.get("genre")

    if search is not None:
        search = re.sub("\W+", "", search, 0, re.IGNORECASE)

    print(search, genre)

    page, per_page, offset = get_page_args(per_page=24)

    if genre is not None:
        query = {"genres": genre}

        movies = moviesCollection.find(query)\
            .skip(offset)\
            .limit(per_page)
        movies_count = moviesCollection.count_documents(query)
    elif search is not None:
        query = {"title": {"$regex": f"{str(search)}", "$options": 'i'}}

        movies = moviesCollection.find(query)\
            .skip(offset)\
            .limit(per_page)
        movies_count = moviesCollection.count_documents(query)
    elif genre is not None and search is not None:
        query = {
            "title": {"$regex": f"{str(search)}", "$options": 'i'}, "genres": genre}

        movies = moviesCollection.find(query)\
            .skip(offset)\
            .limit(per_page)
        movies_count = moviesCollection.count_documents(query)
    else:
        movies = moviesCollection.find({}).skip(offset).limit(per_page)
        movies_count = moviesCollection.count_documents({})

    pagination = Pagination(page=page,
                            per_page=per_page,
                            total=movies_count,
                            record_name='movies')

    genres = ['comedy', 'action', 'drama', 'animation', 'documentary', 'sci-fi', 'adventure', 'crime', 'horror', 'children',
              'uncategories', 'fantasy', 'thriller', 'mystery', 'romance', 'western', 'musical', 'war', 'film-noir']
    dict_genres = {}

    for genre in genres:
        args['genre'] = genre
        url = url_for(request.endpoint, **args)
        dict_genres[genre] = url

    return render_template('ask.html', form=form, movies=list(movies), page=page, genres=dict_genres, pagination=pagination)


@app.route("/fetching", methods=['POST'])
def fetching():
    waitSec = request.get_json()['time']
    session['add_ratings'] = False
    time.sleep(waitSec)
    return jsonify({"status": "ok"})


@ app.errorhandler(Exception)
def error_page(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps({
        "code": e.code,
        "name": e.name,
        "description": e.description,
    })
    response.content_type = "application/json"
    return response


if __name__ == "__main__":
    app.run(debug=True)
