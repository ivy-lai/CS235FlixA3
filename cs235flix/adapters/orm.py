from sqlalchemy import (
    Table, MetaData, Column, Integer, String, Date, DateTime,
    ForeignKey, Float
)
from sqlalchemy.orm import mapper, relationship

from cs235flix.domainmodel import full_model

metadata = MetaData()

movies = Table(
    'movies', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('title', String(255)),
    Column('year', Integer),
    Column('description', String(1024)),
    Column('runtime', Integer),
    Column('ratings', Float),
    Column('votes', Integer),
    Column('revenue', String(255)),
    Column('metascore', String(255)),
    Column('director', ForeignKey('directors.id'))
)

actors = Table(
    'actors', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True)
)

directors = Table(
    'directors', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True)
)

genres = Table(
    'genres', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('name', String(255), unique=True)
)

users = Table(
    'users', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('username', String(255), unique=True, nullable=False),
    Column('password', String(255), nullable=False)
)

reviews = Table(
    'reviews', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('movie_id', ForeignKey('movies.id')),
    Column('user_id', ForeignKey('users.id')),
    Column('review_text', String(1024), nullable=False),
    Column('rating', Integer, nullable=False),
    Column('timestamp', DateTime, nullable=False)

)

movie_genres = Table(
    'movie_genres', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('movie_id', ForeignKey('movies.id')),
    Column('genre_id', ForeignKey('genres.id'))
)

movie_actors = Table(
    'movie_actors', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('movie_id', ForeignKey('movies.id')),
    Column('actor_id', ForeignKey('actors.id'))
)

movie_director = Table(
    'movie_director', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('movie_id', ForeignKey('movies.id')),
    Column('director_id', ForeignKey('directors.id'))
)

review_user = Table(
    'review_user', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('review_id', ForeignKey('reviews.id')),
    Column('user_id', ForeignKey('users.id')),
    Column('movie_id', ForeignKey('movies.id'))
)

Watchlist = Table(
    'watchlist', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('user_id', ForeignKey('users.id')),
    Column('movie_id', ForeignKey('movies.id'))
)


def map_model_to_tables():
    movie_mapper = mapper(full_model.Movie, movies, properties={
        '_Movie__id': movies.c.id,
        '_Movie__title': movies.c.title,
        '_Movie__year': movies.c.year,
        '_Movie__description': movies.c.description,
        '_Movie__runtime': movies.c.runtime,
        '_Movie__ratings': movies.c.ratings,
        '_Movie__reviews': relationship(full_model.Review, backref='_movie'),
        '_Movie__genres': relationship(full_model.Genre, secondary=movie_genres, backref='_movie'),
    })

    mapper(full_model.Actor, actors, properties={
        '_Actor__actor_full_name': actors.c.name,
        '_Actor__starred_movies': relationship(movie_mapper, secondary=movie_actors, backref='_movie')
    })

    mapper(full_model.Director, directors, properties={
        '_Director__director': directors.c.name,
        '_Director__dir_movies': relationship(movie_mapper, secondary=movie_director, backref='_director')
    })

    mapper(full_model.Genre, genres, properties={
        '_Genre__genre_name': genres.c.name,
        '_Genre__tagged_movies': relationship(movie_mapper, secondary=movie_genres, backref='_genre')
    })

    mapper(full_model.User, users, properties={
        '_User__username': users.c.username,
        '_User__password': users.c.password,
        '_User__reviews': relationship(full_model.Review, backref='_user')
    })

    mapper(full_model.Review, reviews, properties={
        '_Review__review_text': reviews.c.review_text,
        '_Review__timestamp': reviews.c.timestamp,
        '_Review__rating': reviews.c.rating
    })