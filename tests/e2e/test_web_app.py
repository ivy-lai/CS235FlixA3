import pytest

from flask import session


def test_register(client):
    # Check that we retrieve the register page.
    response_code = client.get('/authentication/register').status_code
    assert response_code == 200

    # Check that we can register a user successfully, supplying a valid username and password.
    response = client.post(
        '/authentication/register',
        data={'username': 'gmichael', 'password': 'CarelessWhisper1984'}
    )
    assert response.headers['Location'] == 'http://localhost/authentication/login'


@pytest.mark.parametrize(('username', 'password', 'message'), (
        ('', '', b'Your username is required'),
        ('cj', '', b'Your username is too short'),
        ('test', '', b'Your password is required'),
        ('test', 'test', b'Your password must be at least 8 characters, and contain an upper case letter,\
            a lower case letter and a digit')
))
def test_register_with_invalid_input(client, username, password, message):
    # Check that attempting to register with invalid combinations of username and password generate appropriate error
    # messages.
    response = client.post(
        '/authentication/register',
        data={'username': username, 'password': password}
    )
    assert message in response.data


def test_logout(client, auth):
    # Login a user.
    auth.login()

    with client:
        # Check that logging out clears the user's session.
        auth.logout()
        assert 'user_id' not in session


def test_login_required_to_review(client):
    response = client.post('/review')
    assert response.headers['Location'] == 'http://localhost/authentication/login'


def test_review(client, auth):
    # Login a user.
    auth.login()

    # Check that we can retrieve the comment page.
    response = client.get('/review?movie=2')

    response = client.post(
        '/review',
        data={'movie_id': 2, 'review_text': 'Good movie', 'rating': 7}
    )
    assert response.headers['Location'] == 'http://localhost/authentication/login'


@pytest.mark.parametrize(('comment', 'messages'), (
        ('Who thinks Trump is a fuckwit?', (b'Your comment must not contain profanity')),
        ('Hey', (b'Your comment is too short'))
))
def test_comment_with_invalid_input(client, auth, comment, messages):
    # Login a user.
    auth.login()

    # Attempt to comment on an article.
    response = client.post(
        '/comment',
        data={'movie_id': 2, 'review_text': comment, 'rating': 7}
    )
    # Check that supplying invalid comment text generates appropriate error messages.
    for message in messages:
        assert message in response.data


def test_movies_without_id(client):
    # Check that we can retrieve the articles page.
    response = client.get('/movies')
    assert response.status_code == 200
    assert b'Guardians of the Galaxy' in response.data


def test_movies_with_id(client):
    # Check that we can retrieve the articles page.
    response = client.get('/movie_by_id?id=1')
    assert response.status_code == 200

    # Check that all articles on the requested date are included on the page.
    assert b'Guardians of the Galaxy' in response.data
    assert b'Chris Pratt' in response.data
    assert b'2014' in response.data


def test_movies_with_genre(client):
    # Check that we can retrieve the articles page.
    response = client.get('/movies_by_genre?genre=<Genre+Action>')
    assert response.status_code == 200

    # Check that all articles tagged with 'Health' are included on the page.
    assert b'Guardians of the Galaxy' in response.data


def test_movies_with_actor(client):
    # Check that we can retrieve the articles page.
    response = client.get('/movies_by_actor?actor=<Actor+Chris+Pratt>')
    assert response.status_code == 200

    # Check that all articles tagged with 'Health' are included on the page.
    assert b'Guardians of the Galaxy' in response.data


def test_movies_with_director(client):
    # Check that we can retrieve the articles page.
    response = client.get('/movies_by_director?director=<Director+James+Gunn>')
    assert response.status_code == 200

    # Check that all articles tagged with 'Health' are included on the page.
    assert b'Guardians of the Galaxy' in response.data


def test_search(client):
    # Check that we can retrieve the articles page.
    response = client.get('/search?search=&select=All')
    assert response.status_code == 200
    assert b'No results found' in response.data


