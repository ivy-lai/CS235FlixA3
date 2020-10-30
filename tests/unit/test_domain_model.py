
from datetime import date

from cs235flix.domainmodel.full_model import Actor, Director, Genre, Movie, Review, User, WatchList, make_review, \
    make_genre_association, ModelException, make_director_association, make_actor_association

import pytest


@pytest.fixture()
def movie():
    return Movie(
        6,
        'The Great Wall',
        2016,
        'European mercenaries searching for black powder become embroiled in the defense of the Great Wall of China '
        'against a horde of monstrous creatures.',
        103,
        6.1
    )


@pytest.fixture()
def user():
    return User('dbowie', '1234567890')


@pytest.fixture()
def genre():
    return Genre('Action')


@pytest.fixture()
def director():
    return Director('Yimou Zhang')


@pytest.fixture()
def actor():
    return Actor('Matt Damon')


def test_user_construction(user):
    assert user.username == 'dbowie'
    assert user.password == '1234567890'
    assert repr(user) == '<User dbowie>'

    for review in user.reviews:
        # User should have an empty list of Comments after construction.
        assert False


def test_movie_construction(movie):
    assert movie.id is 6
    assert movie.title == 'The Great Wall'
    assert movie.year == 2016
    assert movie.description == 'European mercenaries searching for black powder become embroiled in the defense of the Great Wall of China against a horde of monstrous creatures.'
    assert movie.runtime == 103

    assert movie.number_of_reviews == 0
    assert movie.number_of_genres == 0
    assert movie.ratings == 6.1

    assert repr(
        movie) == '<Movie The Great Wall, 2016>'


def test_movies_less_than_operator():
    movie_1 = Movie(
        None, 'Hot Pursuit', 1987, None, None, None
    )

    movie_2 = Movie(
        None, 'Hot Pursuit', 2015, None, None, None
    )

    assert movie_1 < movie_2


def test_genre_construction(genre):
    assert genre.name == 'Action'

    for movie in genre.tagged_movies:
        assert False

    assert not genre.is_applied_to(Movie(None, None, None, None, None, None))


def test_director_construction(director):
    assert director.name == 'Yimou Zhang'

    for movie in director.director_movies:
        assert False

    assert not director.is_applied_to(Movie(None, None, None, None, None, None))


def test_actor_construction(actor):
    assert actor.name == 'Matt Damon'

    for movie in actor.starred_movies:
        assert False

    assert not actor.is_applied_to(Movie(None, None, None, None, None, None))


def test_make_review_establishes_relationships(movie, user):
    review_text = 'The best movie ever!'
    rating = 10
    review = make_review(user, movie, review_text, rating)

    # Check that the User object knows about the Comment.
    assert review in user.reviews

    # Check that the Comment knows about the User.
    assert review.user is user

    # Check that Article knows about the Comment.
    assert review in movie.reviews

    # Check that the Comment knows about the Article.
    assert review.movie is movie


def test_make_genre_associations(movie, genre):
    make_genre_association(movie, genre)

    # Check that the Movie knows about the Genre.
    assert movie.is_tagged()
    assert movie.is_tagged_by(genre)

    # check that the Genre knows about the Movie.
    assert genre.is_applied_to(movie)
    assert movie in genre.tagged_movies


def test_make_director_associations(movie, director):
    make_director_association(movie, director)

    # Check that the Movie knows about the Director.
    assert movie.director == director

    # check that the Director knows about the Movie.
    assert director.is_applied_to(movie)
    assert movie in director.director_movies


def test_make_actor_associations(movie, actor):
    make_actor_association(movie, actor)

    # Check that the Movie knows about the Genre.
    assert movie.is_actor_in(actor)
    assert movie.is_actor()

    # check that the Genre knows about the Movie.
    assert actor.is_applied_to(movie)
    assert movie in actor.starred_movies


def test_make_genre_associations_with_movie_already_tagged(movie, genre):
    make_genre_association(movie, genre)

    with pytest.raises(ModelException):
        make_genre_association(movie, genre)


def test_make_director_associations_with_movie_already_done(movie, director):
    make_director_association(movie, director)

    with pytest.raises(ModelException):
        make_director_association(movie, director)


def test_make_actor_associations_with_movie_already_done(movie, actor):
    make_actor_association(movie, actor)

    with pytest.raises(ModelException):
        make_actor_association(movie, actor)