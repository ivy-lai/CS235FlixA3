from typing import Iterable

from cs235flix.adapters.repository import AbstractRepository
from cs235flix.domainmodel.full_model import Movie, Genre, make_review, Review, Actor, Director


class NonExistentMovieException(Exception):
    pass


class UnknownUserException(Exception):
    pass


def get_watchlist(repo: AbstractRepository):
    movies = repo.get_watchlist()
    movies_dto = list()
    if len(movies) > 0:
        movies_dto = movies_to_dict(movies)
    return movies_dto


def add_watchlist(movie_id: int, repo:AbstractRepository):
    movie = repo.get_movie_by_id(movie_id)
    if movie is None:
        raise NonExistentMovieException
    repo.add_watchlist(movie)


def remove_watchlist(movie_id: int, repo:AbstractRepository):
    movie = repo.get_movie_by_id(movie_id)
    if movie is None:
        raise NonExistentMovieException
    repo.remove_watchlist(movie)


def add_review(username: str, movie_id: int, review_text: str, rating: int, repo: AbstractRepository):
    # Check that the movie exists.
    movie = repo.get_movie_by_id(movie_id)
    if movie is None:
        raise NonExistentMovieException

    user = repo.get_user(username)
    if user is None:
        raise UnknownUserException

    # Create comment.
    review = make_review(user, movie, review_text, rating)

    # Update the repository.
    repo.add_review(review)


def get_movies_by_genre(movie: str, repo: AbstractRepository):
    movie = repo.get_movie(movie.__repr__())

    if movie is None:
        raise NonExistentMovieException

    return movie_to_dict(movie)


def get_genre(genre, repo: AbstractRepository):
    genre = repo.get_genre()

    if genre is None:
        raise NonExistentMovieException

    return genre


def get_first_movie(repo: AbstractRepository):
    movie = repo.get_first_movie()
    return movie_to_dict(movie)


def get_last_movie(repo: AbstractRepository):
    movie = repo.get_last_movie()
    return movie_to_dict(movie)


def get_movies_by_ids(id_list, repo: AbstractRepository):
    movies = repo.get_movies_by_ids(id_list)
    movies_as_dict = movies_to_dict(movies)
    return movies_as_dict


def get_movie_by_id(id, repo: AbstractRepository):
    movies = repo.get_movie_by_id(id)
    movie_as_dict = movie_to_dict(movies)
    return movie_as_dict


def get_movie_for_genre(genre_name, repo: AbstractRepository):
    movies = repo.get_movie_for_genre(genre_name)
    movies_dto = list()
    if len(movies) > 0:
        movies_dto = movies_to_dict(movies)

    return movies_dto


def get_movie_by_director(director_name, repo: AbstractRepository):
    movies = repo.get_movie_by_director(director_name)
    movies_dto = list()
    if len(movies) > 0:
        movies_dto = movies_to_dict(movies)

    return movies_dto


def get_movie_for_actor(actor_name, repo: AbstractRepository):
    movies = repo.get_movie_for_actor(actor_name)
    movies_dto = list()
    if len(movies) > 0:
        movies_dto = movies_to_dict(movies)

    return movies_dto


def get_all_movies(repo: AbstractRepository):
    movies = repo.get_all_movies()
    movies_dto = list()
    if len(movies) > 0:
        movies_dto = movies_to_dict(movies)

    return movies_dto


def get_reviews_for_movie(movie_id, repo: AbstractRepository):
    movie = repo.get_movie_by_id(movie_id)

    if movie is None:
        raise NonExistentMovieException

    return reviews_to_dict(movie.reviews)


def get_actors_for_movie(movie_id, repo: AbstractRepository):
    movie = repo.get_movie_by_id(movie_id)

    if movie is None:
        raise NonExistentMovieException

    return actors_to_dict(movie.actors)


def get_director_for_movie(movie_id, repo: AbstractRepository):
    movie = repo.get_movie_by_id(movie_id)

    if movie is None:
        raise NonExistentMovieException

    return director_to_dict(movie.director)


# ============================================
# Functions to convert model entities to dicts
# ============================================

def movie_to_dict(movie: Movie):
    movie_dict = {
        'id': movie.id,
        'title': movie.title,
        'year': movie.year,
        'description': movie.description,
        'actors': actors_to_dict(movie.actors),
        'director': director_to_dict(movie.director),
        'genres': genres_to_dict(movie.genres),
        'runtime': movie.runtime,
        'reviews': reviews_to_dict(movie.reviews),
        'ratings': movie.ratings,
    }
    return movie_dict


def movies_to_dict(movies: Iterable[Movie]):
    return [movie_to_dict(movie) for movie in movies]


def review_to_dict(review: Review):
    review_dict = {
        'username': review.user.username,
        'movie_id': review.movie.id,
        'review_text': review.review_text,
        'rating': review.rating,
        'timestamp': review.timestamp
    }
    return review_dict


def reviews_to_dict(reviews: Iterable[Review]):
    return [review_to_dict(review) for review in reviews]


def genre_to_dict(genre: Genre):
    genre_dict = {
        'name': genre.name,
        'tagged_movie': [movie.__repr__() for movie in genre.tagged_movies]
    }
    return genre_dict


def genres_to_dict(genres: Iterable[Genre]):
    return [genre_to_dict(genre) for genre in genres]


def actor_to_dict(actor: Actor):
    actor_dict = {
        'name': actor.name
    }
    return actor_dict


def actors_to_dict(actors: Iterable[Actor]):
    return [actor_to_dict(actor) for actor in actors]


def director_to_dict(director: Director):
    director_dict = {
        'name': director.name
    }

    return director_dict


# ============================================
# Functions to convert dicts to model entities
# ============================================

def dict_to_movie(dict):
    movie = Movie(dict.id, dict.title, dict.genre)
    return movie
