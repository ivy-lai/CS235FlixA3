from datetime import date, datetime
from typing import List

import pytest

from cs235flix.adapters.database_repository import SqlAlchemyRepository
from cs235flix.domainmodel.full_model import User, Movie, Genre, Review, make_review, Director, Actor
from cs235flix.adapters.repository import RepositoryException


def test_repository_can_retrieve_movie_count(session_factory):
    repo = SqlAlchemyRepository(session_factory)

    number_of_movies = repo.get_number_of_movies()

    # Check that the query returned 5 Movies.
    assert number_of_movies == 5


def test_repository_can_add_movie(session_factory):
    repo = SqlAlchemyRepository(session_factory)

    number_of_movies = repo.get_number_of_movies()

    new_movie_id = number_of_movies + 1

    movie = Movie(new_movie_id, 'The Great Wall', 2016, 'European mercenaries searching for black powder become '
                                             'embroiled in the defense of the Great Wall of China against a '
                                             'horde of monstrous creatures.', 103, 6.1)
    repo.add_movie(movie)


def test_repository_can_retrieve_movie_by_id(session_factory):
    repo = SqlAlchemyRepository(session_factory)

    movie = repo.get_movie_by_id(1)

    # Check that the Movie has the expected title.
    assert movie.title == 'Guardians of the Galaxy'
    assert movie.year == 2014


def test_repository_does_not_retrieve_a_non_existent_movie(session_factory):
    repo = SqlAlchemyRepository(session_factory)
    movie = repo.get_movie_by_id(8)
    assert movie is None


def test_repository_can_retrieve_movie_by_name_and_year(session_factory):
    repo = SqlAlchemyRepository(session_factory)
    movie = repo.get_movie('Suicide Squad', 2016)

    # Check that the Movie has the expected title.
    assert movie.title == 'Suicide Squad'

    # Check that Movie has expected run time
    assert movie.runtime == 123


def test_repository_does_not_retrieve_a_movie_when_there_are_no_movie_for_a_name_and_year(session_factory):
    repo = SqlAlchemyRepository(session_factory)
    movie = repo.get_movie('Moana', 2016)
    assert movie is None


def test_repository_can_get_movies_by_ids(session_factory):
    repo = SqlAlchemyRepository(session_factory)
    movies = repo.get_movies_by_id([2, 5, 4])

    assert len(movies) == 3
    assert movies[0].title == 'Prometheus'
    assert movies[1].runtime == 108
    assert movies[2].ratings == 6.2


def test_repository_does_not_retrieve_movies_for_non_existent_id(session_factory):
    repo = SqlAlchemyRepository(session_factory)
    movies = repo.get_movies_by_id([2, 9])

    assert len(movies) == 1
    assert movies[0].title == 'Prometheus'


def test_repository_returns_an_empty_list_for_non_existent_ids(session_factory):
    repo = SqlAlchemyRepository(session_factory)
    articles = repo.get_movies_by_id([0, 9])

    assert len(articles) == 0


def test_repository_can_add_a_genre(session_factory):
    repo = SqlAlchemyRepository(session_factory)
    genre = Genre('Sports')
    repo.add_genre(genre)

    assert genre in repo.get_genres()


