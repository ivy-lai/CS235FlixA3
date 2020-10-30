import abc
from typing import List

from cs235flix.domainmodel.full_model import Movie, Actor, Director, Genre, Review, User, WatchList


repo_instance = None


class RepositoryException(Exception):

    def __init__(self, message=None):
        pass


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def add_movie(self, movie: Movie):
        raise NotImplementedError

    @abc.abstractmethod
    def get_movie(self, title: str, year: int) -> Movie:
        raise NotImplementedError

    @abc.abstractmethod
    def add_director(self, director):
        raise NotImplementedError

    @abc.abstractmethod
    def get_directors(self):
        raise NotImplementedError

    @abc.abstractmethod
    def add_genre(self, genre_name):
        raise NotImplementedError

    @abc.abstractmethod
    def get_genres(self):
        raise NotImplementedError

    @abc.abstractmethod
    def add_actor(self, actor_name):
        raise NotImplementedError

    @abc.abstractmethod
    def get_actors(self):
        raise NotImplementedError

    @abc.abstractmethod
    def add_user(self, user):
        raise NotImplementedError

    @abc.abstractmethod
    def get_user(self, username):
        raise NotImplementedError

    @abc.abstractmethod
    def add_review(self, review: Review):
        if review.user is None or review not in review.user.reviews:
            raise RepositoryException('Review not correctly attached to a User')
        if review.movie is None or review not in review.movie.reviews:
            raise RepositoryException('Review not correctly attached to a Movie')

    @abc.abstractmethod
    def get_reviews(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_movie_by_id(self, movie_id: int) -> Movie:
        raise NotImplementedError

    @abc.abstractmethod
    def get_movies_by_id(self, id_list) -> Movie:
        raise NotImplementedError

    @abc.abstractmethod
    def get_movie_for_genre(self, genre_name):
        raise NotImplementedError

    @abc.abstractmethod
    def get_movie_id_by_genre(self, genre_name: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_movies(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_number_of_movies(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_ratings(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_ids(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_movie_for_actor(self, actor):
        raise NotImplementedError

    @abc.abstractmethod
    def get_movie_by_director(self, director):
        raise NotImplementedError
"""
    @abc.abstractmethod
    def get_watchlist(self):
        raise NotImplementedError

    @abc.abstractmethod
    def add_watchlist(self, movie):
        raise NotImplementedError

    @abc.abstractmethod
    def remove_watchlist(self, movie):
        raise NotImplementedError
    """