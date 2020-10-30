from datetime import date

import pytest

from cs235flix.authentication.services import AuthenticationException
from cs235flix.movies import services as movies_services
from cs235flix.authentication import services as auth_services
from cs235flix.movies.services import NonExistentMovieException
from cs235flix.authentication.services import NameNotUniqueException
from cs235flix.domainmodel.full_model import User


def test_can_add_user(in_memory_repo):
    new_username = 'jz'
    new_password = 'abcd1A23'

    auth_services.add_user(new_username, new_password, in_memory_repo)

    user_as_dict = auth_services.get_user(new_username, in_memory_repo)
    assert user_as_dict['username'] == new_username

    # Check that password has been encrypted.
    assert user_as_dict['password'].startswith('pbkdf2:sha256:')

"""
def test_cannot_add_user_with_existing_name(in_memory_repo):
    username = 'thorke'
    password = 'abcd1A23'

    with pytest.raises(auth_services.NameNotUniqueException):
        auth_services.add_user(username, password, in_memory_repo)
"""


def test_authentication_with_valid_credentials(in_memory_repo):
    new_username = 'pmccartney'
    new_password = 'abcd1A23'

    auth_services.add_user(new_username, new_password, in_memory_repo)

    try:
        auth_services.authenticate_user(new_username, new_password, in_memory_repo)
    except AuthenticationException:
        assert False


def test_authentication_with_invalid_credentials(in_memory_repo):
    new_username = 'pmccartney'
    new_password = 'abcd1A23'

    auth_services.add_user(new_username, new_password, in_memory_repo)

    with pytest.raises(auth_services.AuthenticationException):
        auth_services.authenticate_user(new_username, '0987654321', in_memory_repo)

"""
def test_can_add_review(in_memory_repo):
    movie_id = 3
    rating = 7
    review_text = 'Very interesting movie!'
    username = 'Dave'
    # Call the service layer to add the comment.
    movies_services.add_review(username, movie_id, review_text, rating, in_memory_repo)

    # Retrieve the comments for the article from the repository.
    reviews_as_dict = movies_services.get_reviews_for_movie(movie_id, in_memory_repo)

    # Check that the comments include a comment with the new comment text.
    assert next(
        (dictionary['review_text'] for dictionary in reviews_as_dict if dictionary['review_text'] == review_text),
        None) is not None
"""


def test_cannot_add_review_for_non_existent_article(in_memory_repo):
    movie_id = 8
    review_text = "Great Movie"
    rating = 7
    username = 'fmercury'

    # Call the service layer to attempt to add the comment.
    with pytest.raises(movies_services.NonExistentMovieException):
        movies_services.add_review(username, movie_id, review_text, rating, in_memory_repo)


def test_cannot_add_review_by_unknown_user(in_memory_repo):
    movie_id = 8
    review_text = "Average Movie"
    rating = 7
    username = 'gmichael'

    # Call the service layer to attempt to add the comment.
    with pytest.raises(movies_services.NonExistentMovieException):
        movies_services.add_review(username, movie_id, review_text, rating, in_memory_repo)


def test_can_get_movie(in_memory_repo):
    movie_id = 5

    movie_as_dict = movies_services.get_movie_by_id(movie_id, in_memory_repo)

    assert movie_as_dict['id'] == movie_id
    assert movie_as_dict['title'] == 'Suicide Squad'
    assert movie_as_dict['year'] == 2016
    assert movie_as_dict['description'] == 'A secret government agency recruits some of the most dangerous incarcerated super-villains to form a defensive task force. Their first mission: save the world from the apocalypse.'
    assert movie_as_dict['runtime'] == 123

    genre_names = [dictionary['name'] for dictionary in movie_as_dict['genres']]
    assert 'Action' in genre_names
    assert 'Adventure' in genre_names
    assert 'Fantasy' in genre_names

    actor_names = [dictionary['name'] for dictionary in movie_as_dict['actors']]
    assert 'Will Smith' in actor_names
    assert 'Jared Leto' in actor_names
    assert 'Margot Robbie' in actor_names
    assert 'Viola Davis' in actor_names
