import csv
import os

from datetime import date
from typing import List

from sqlalchemy import desc, asc
from sqlalchemy.engine import Engine
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from werkzeug.security import generate_password_hash

from sqlalchemy.orm import scoped_session
from flask import _app_ctx_stack

from cs235flix.domainmodel.full_model import Movie, Actor, Director, Genre, User, make_genre_association, \
    make_director_association, make_actor_association, Review, WatchList
from cs235flix.adapters.repository import AbstractRepository

genres = None


class SessionContextManager:
    def __init__(self, session_factory):
        self.__session_factory = session_factory
        self.__session = scoped_session(self.__session_factory, scopefunc=_app_ctx_stack.__ident_func__)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.rollback()

    @property
    def session(self):
        return self.__session

    def commit(self):
        self.__session.commit()

    def rollback(self):
        self.__session.rollback()

    def reset_session(self):
        # this method can be used e.g. to allow Flask to start a new session for each http request,
        # via the 'before_request' callback
        self.close_current_session()
        self.__session = scoped_session(self.__session_factory, scopefunc=_app_ctx_stack.__ident_func__)

    def close_current_session(self):
        if not self.__session is None:
            self.__session.close()


class SqlAlchemyRepository(AbstractRepository):

    def __init__(self, session_factory):
        self._session_cm = SessionContextManager(session_factory)

    def close_session(self):
        self._session_cm.close_current_session()

    def reset_session(self):
        self._session_cm.reset_session()

    def add_user(self, user: User):
        with self._session_cm as scm:
            scm.session.add(user)
            scm.commit()

    def get_user(self, username) -> User:
        user = None
        try:
            user = self._session_cm.session.query(User).filter_by(username=username).all()
        except NoResultFound:
            # Ignore any exception and return None.
            pass

        return user

    def add_actor(self, actor: Actor):
        with self._session_cm as scm:
            scm.session.add(actor)
            scm.commit()

    def get_actors(self, actor) -> Actor:
        actor = None
        try:
            actor = self._session_cm.session.query(Actor).filter_by(actor_name=actor).one()
        except NoResultFound:
            # Ignore any exception and return None.
            pass
        return actor

    def add_director(self, director: Director):
        with self._session_cm as scm:
            scm.session.add(director)
            scm.commit()

    def get_directors(self, director) -> Director:
        director = None
        try:
            director = self._session_cm.session.query(Director).filter_by(director_name=director).one()
        except NoResultFound:
            # Ignore any exception and return None.
            pass
        return director

    def add_watchlist(self, movie):
        with self._session_cm as scm:
            scm.session.add(movie)
            scm.commit()

    def add_movie(self, movie: Movie):
        with self._session_cm as scm:
            scm.session.add(movie)
            scm.commit()

    def get_movie_by_id(self, id: int) -> Movie:
        movie = None
        try:
            movies = self._session_cm.session.query(Movie)
            for m in movies:
                if m.id == id:
                    movie = m
        except NoResultFound:
            # Ignore any exception and return None.
            pass
        return movie

    def get_movie_by_director(self, director):
        if director is None:
            movies = self._session_cm.session.query(Director).all()
            return movies
        else:
            # Return articles matching target_date; return an empty list if there are no matches.
            movies = self._session_cm.session.query(Director).filter(Movie.director == director).all()
            return movies

    def get_movie_for_actor(self, actor):
        if actor is None:
            movies = self._session_cm.session.query(Actor).all()
            return movies
        else:
            # Return articles matching target_date; return an empty list if there are no matches.
            movies = self._session_cm.session.query(Actor).filter(actor.in_(Movie.actors)).all()
            return movies

    def get_all_movies(self) -> Movie:
        movie = None
        try:
            movie = self._session_cm.session.query(Movie).all()
        except NoResultFound:
            # Ignore any exception and return None.
            pass
        return movie

    def get_ids(self) -> Movie:
        ids = None
        try:
            ids = self._session_cm.session.query(Movie.id).all()
        except NoResultFound:
            # Ignore any exception and return None.
            pass
        return ids

    def get_movie(self, title: str, year: int) -> Movie:
        movies = self._session_cm.session.query(Movie).all()
        for movie in movies:
            if movie.title == title and movie.year == year:
                return movie

    def get_movie_for_genre(self, genre_name):
        movie = self._session_cm.session.query(Movie).filter(genre_name.in_(Movie.genres)).all()
        return movie

    def get_ratings(self):
        ratings = self._session_cm.query(Movie.ratings).all()
        return ratings

    def get_number_of_movies(self):
        number_of_movies = self._session_cm.session.query(Movie).count()
        return number_of_movies

    def get_movies_by_id(self, id_list) -> Movie:
        movies_list = []
        movies = self._session_cm.session.query(Movie).all()
        for movie in movies:
            if movie.id in id_list:
                movies_list.append(movie)
        return movies_list

    def get_movie_id_by_genre(self, genre_name: str):
        genre_ids = []

        # Use native SQL to retrieve article ids, since there is no mapped class for the article_tags table.
        row = self._session_cm.session.execute('SELECT id FROM genres WHERE name = :genre_name', {'genre_name': genre_name}).fetchone()

        if row is None:
            # No tag with the name tag_name - create an empty list.
            genre_ids = list()
        else:
            genre_id = row[0]

            # Retrieve article ids of articles associated with the tag.
            genre_ids = self._session_cm.session.execute(
                    'SELECT movie_id FROM movie_tags WHERE genre_id = :genre_id ORDER BY movie_id ASC',
                    {'genre_id': genre_id}
            ).fetchall()
            genre_ids = [id[0] for id in genre_ids]

        return genre_ids

    def get_genres(self) -> List[Genre]:
        genre_list = self._session_cm.session.query(Genre).all()
        return genre_list

    def add_genre(self, genre_name: Genre):
        with self._session_cm as scm:
            scm.session.add(genre_name)
            scm.commit()

    def get_reviews(self) -> List[Review]:
        reviews = self._session_cm.session.query(Review).all()
        return reviews

    def add_review(self, review: Review):
        super().add_review(review)
        with self._session_cm as scm:
            scm.session.add(review)
            scm.commit()


def movie_record_generator(filename: str):
    with open(filename, mode='r', encoding='utf-8-sig') as infile:
        reader = csv.reader(infile)

        # Read first line of the CSV file.
        headers = next(reader)

        # Read remaining rows from the CSV file.
        for row in reader:

            movie_data = row
            movie_key = movie_data[0]

            # Strip any leading/trailing white space from data read.
            movie_data = [item.strip() for item in movie_data]
            movie_genres = movie_data[2]

            # Add any new genre; associate the current movie with genres.
            for genre in movie_genres:
                if genre not in genre.keys():
                    genres[genre] = list()
                genres[genre].append(movie_key)

            row = [row[0], row[1], row[6], row[3], row[7], row[8]]
            yield row


def get_genre_records():
    genre_records = list()
    genre_key = 0

    for genre in genres.keys():
        genre_key = genre_key + 1
        genre_records.append((genre_key, genre))
    return genre_records


def movie_genres_generator():
    movie_genres_key = 0
    genre_key = 0

    for genre in genres.keys():
        genre_key = genre_key + 1
        for movie_key in genres[genre]:
            movie_genres_key = movie_genres_key + 1
            yield movie_genres_key, movie_key, genre_key


def generic_generator(filename, post_process=None):
    with open(filename) as infile:
        reader = csv.reader(infile)

        # Read first line of the CSV file.
        next(reader)

        # Read remaining rows from the CSV file.
        for row in reader:
            # Strip any leading/trailing white space from data read.
            row = [row[0], row[1], row[6], row[3], row[7], row[8]]
            # row = [item.strip() for item in row]

            if post_process is not None:
                row = post_process(row)
            yield row


def process_user(user_row):
    user_row[2] = generate_password_hash(user_row[2])
    return user_row


def populate(engine: Engine, data_path: str):
    conn = engine.raw_connection()
    cursor = conn.cursor()

    global genres
    genres = dict()

    insert_movies = """
        INSERT INTO movies (
        id, title, year, description, runtime, ratings)
        VALUES (?, ?, ?, ?, ?, ?)"""
    cursor.executemany(insert_movies, generic_generator(os.path.join(data_path, 'Data1000Movies.csv')))

    insert_genres = """
        INSERT INTO genres (
        id, name)
        VALUES (?, ?)"""
    cursor.executemany(insert_genres, get_genre_records())

    insert_movie_genres = """
        INSERT INTO movie_genres (
        id, movie_id, genre_id)
        VALUES (?, ?, ?)"""
    cursor.executemany(insert_movie_genres, movie_genres_generator())

    """
    insert_users = ""
        INSERT INTO users (
        id, username, password)
        VALUES (?, ?, ?)""
    cursor.executemany(insert_users, generic_generator(os.path.join(data_path, 'users.csv'), process_user))

    insert_comments = ""
        INSERT INTO comments (
        id, user_id, article_id, comment, timestamp)
        VALUES (?, ?, ?, ?, ?)""
    cursor.executemany(insert_comments, generic_generator(os.path.join(data_path, 'comments.csv')))
 """

    conn.commit()
    conn.close()

