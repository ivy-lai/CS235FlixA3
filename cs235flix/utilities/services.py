from typing import Iterable
import random

from cs235flix.adapters.repository import AbstractRepository
from cs235flix.domainmodel.full_model import Movie


def get_ids(repo: AbstractRepository):
    ids = repo.get_ids()
    return ids


def get_genre(repo: AbstractRepository):
    genres = repo.get_genres()
    return genres


def get_directors(repo: AbstractRepository):
    directors = repo.get_directors()
    return directors


def get_actors(repo: AbstractRepository):
    actors = repo.get_actors()
    return actors


def get_random_movie(quantity, repo: AbstractRepository):
    movie_count = int(repo.get_number_of_movies())

    if quantity >= movie_count:
        # Reduce the quantity of ids to generate if the repository has an insufficient number of articles.
        quantity = movie_count - 1

    # Pick distinct and random articles.
    random_ids = random.sample(range(1, movie_count), quantity)
    movies = repo.get_movies_by_id(random_ids)

    return movies_to_dict(movies)


# ============================================
# Functions to convert dicts to model entities
# ============================================

def movie_to_dict(movie: Movie):
    movie_dict = {
        'title': movie.title,
        'year': movie.year
    }
    return movie_dict


def movies_to_dict(movies: Iterable[Movie]):
    return [movie_to_dict(movie) for movie in movies]