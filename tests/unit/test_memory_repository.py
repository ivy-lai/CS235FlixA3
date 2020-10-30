from datetime import date, datetime
from typing import List

import pytest

from cs235flix.domainmodel.full_model import Actor, Director, Genre, Movie, Review, User, WatchList, make_review
from cs235flix.adapters.repository import RepositoryException


def test_repository_can_add_a_user(in_memory_repo):
    user = User('Dave', '123456789')
    in_memory_repo.add_user(user)

    assert in_memory_repo.get_user('Dave') is user


def test_repository_can_retrieve_a_user(in_memory_repo):
    user = User('Dave', '123456789')
    in_memory_repo.add_user(user)
    user = in_memory_repo.get_user('Dave')
    assert user == User('Dave', '123456789')


def test_repository_does_not_retrieve_a_non_existent_user(in_memory_repo):
    user = in_memory_repo.get_user('prince')
    assert user is None


def test_repository_can_retrieve_movie_count(in_memory_repo):
    number_of_movies = in_memory_repo.get_number_of_movies()

    # Check that the query returned 5 Movies.
    assert number_of_movies == 5


def test_repository_can_add_a_movie(in_memory_repo):
    movie = Movie(6, 'The Great Wall', 2016, 'European mercenaries searching for black powder become embroiled in the '
                                             'defense of the Great Wall of China against a horde of monstrous '
                                             'creatures.', 103, 6.1)

    in_memory_repo.add_movie(movie)

    assert in_memory_repo.get_movie('The Great Wall', 2016) is movie
    assert in_memory_repo.get_movie_by_id(6) is movie


def test_repository_can_retrieve_movie_by_id(in_memory_repo):
    movie = in_memory_repo.get_movie_by_id(1)

    # Check that the Movie has the expected title.
    assert movie.title == 'Guardians of the Galaxy'

    # Check that the Movie has expected genre.
    assert movie.is_tagged_by(Genre('Action'))
    assert movie.is_tagged_by(Genre('Adventure'))
    assert movie.is_tagged_by(Genre('Sci-Fi'))


def test_repository_does_not_retrieve_a_non_existent_movie(in_memory_repo):
    movie = in_memory_repo.get_movie_by_id(8)
    assert movie is None


def test_repository_can_retrieve_movie_by_name_and_year(in_memory_repo):
    movie = in_memory_repo.get_movie('Suicide Squad', 2016)

    # Check that the Movie has the expected title.
    assert movie.title == 'Suicide Squad'

    # Check that the Movie has expected genre.
    assert movie.genres == [Genre('Action'), Genre('Adventure'), Genre('Fantasy')]

    # Check that the Movie has expected actors.
    assert movie.actors == [Actor('Will Smith'), Actor('Jared Leto'), Actor('Margot Robbie'), Actor('Viola Davis')]

    # Check that Movie has expected director
    assert movie.director == Director('David Ayer')

    # Check that Movie has expected run time
    assert movie.runtime == 123


def test_repository_does_not_retrieve_a_movie_when_there_are_no_movie_for_a_name_and_year(in_memory_repo):
    movie = in_memory_repo.get_movie('Moana', 2016)
    assert movie is None


def test_repository_can_retrieve_genres(in_memory_repo):
    genres: List[Genre] = in_memory_repo.get_genres()

    assert len(genres) == 10

    genre_one = [genre for genre in genres if genre.name == 'Action'][0]
    genre_two = [genre for genre in genres if genre.name == 'Horror'][0]
    genre_three = [genre for genre in genres if genre.name == 'Adventure'][0]
    genre_four = [genre for genre in genres if genre.name == 'Sci-Fi'][0]

    assert genre_one.number_of_tagged_movies == 2
    assert genre_two.number_of_tagged_movies == 1
    assert genre_three.number_of_tagged_movies == 3
    assert genre_four.number_of_tagged_movies == 2


def test_repository_can_get_movies_by_ids(in_memory_repo):
    movies = in_memory_repo.get_movies_by_id([2, 5, 4])

    assert len(movies) == 3
    assert movies[0].title == 'Prometheus'
    assert movies[1].runtime == 123
    assert movies[2].director == Director('Christophe Lourdelet')


def test_repository_does_not_retrieve_movies_for_non_existent_id(in_memory_repo):
    movies = in_memory_repo.get_movies_by_id([2, 9])

    assert len(movies) == 1
    assert movies[0].title == 'Prometheus'


def test_repository_returns_an_empty_list_for_non_existent_ids(in_memory_repo):
    articles = in_memory_repo.get_movies_by_id([0, 9])

    assert len(articles) == 0


def test_repository_returns_movies_ids_for_existing_tag(in_memory_repo):
    movie_ids = in_memory_repo.get_movie_id_by_genre('Action')

    assert movie_ids == [1, 5]


def test_repository_returns_an_empty_list_for_non_existent_tag(in_memory_repo):
    movie_ids = in_memory_repo.get_movie_id_by_genre('United States')

    assert len(movie_ids) == 0


def test_repository_can_add_a_genre(in_memory_repo):
    genre = Genre('Sports')
    in_memory_repo.add_genre(genre)

    assert genre in in_memory_repo.get_genres()


def test_repository_can_add_a_review(in_memory_repo):
    user = User('Dave', '123456789')
    in_memory_repo.add_user(user)
    assert in_memory_repo.get_user('Dave') is user

    movie = in_memory_repo.get_movie_by_id(2)
    review = make_review(user, movie, "Great movie", 5, datetime.today())

    in_memory_repo.add_review(review)

    assert review in in_memory_repo.get_reviews()

"""
def test_repository_does_not_add_a_review_without_a_user(in_memory_repo):
    movie = in_memory_repo.get_movie_by_id(2)
    review = Review(None, movie, "Great movie", 5, datetime.today())

    with pytest.raises(RepositoryException):
        in_memory_repo.add_review(review)


def test_repository_does_not_add_a_review_without_an_movie_properly_attached(in_memory_repo):
    user = User('Dave', '123456789')
    in_memory_repo.add_user(user)

    movie = in_memory_repo.get_movie_by_id(2)
    review = Review(None, movie, "Great movie", 5, datetime.today())

    user.add_review(review)

    with pytest.raises(RepositoryException):
        # Exception expected because the Article doesn't refer to the Comment.
        in_memory_repo.add_review(review)
"""


def test_repository_can_retrieve_reviews(in_memory_repo):
    user = User('Dave', '123456789')
    in_memory_repo.add_user(user)

    movie = in_memory_repo.get_movie_by_id(2)
    review = make_review(user, movie, "Great movie", 5, datetime.today())
    in_memory_repo.add_review(review)

    movie = in_memory_repo.get_movie_by_id(4)
    review = make_review(user, movie, "Very funny movie", 7, datetime.today())
    in_memory_repo.add_review(review)

    assert len(in_memory_repo.get_reviews()) == 2
