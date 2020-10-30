import pytest

import datetime

from sqlalchemy.exc import IntegrityError

from cs235flix.domainmodel.full_model import User, Movie, Actor, Director, Review, Genre, make_review, \
    make_genre_association, \
    make_actor_association, make_director_association


def make_genre():
    genre = Genre("Sport")
    return genre


def make_movie():
    movie = Movie(6, 'The Great Wall', 2016, 'European mercenaries searching for black powder become '
                                             'embroiled in the defense of the Great Wall of China against a '
                                             'horde of monstrous creatures.', 103, 6.1)
    return movie


def make_user():
    user = User("Andrew", "111")
    return user


def make_actor():
    actor = Actor("Will Smith")
    return actor


def make_director():
    director = Director("David Ayer")
    return director


def insert_user(empty_session, values=None):
    new_name = "Andrew"
    new_password = "1234"

    if values is not None:
        new_name = values[0]
        new_password = values[1]

    empty_session.execute('INSERT INTO users (username, password) VALUES (:username, :password)',
                          {'username': new_name, 'password': new_password})
    row = empty_session.execute('SELECT id from users where username = :username',
                                {'username': new_name}).fetchone()
    return row[0]


def insert_users(empty_session, values):
    for value in values:
        empty_session.execute('INSERT INTO users (username, password) VALUES (:username, :password)',
                              {'username': value[0], 'password': value[1]})
    rows = list(empty_session.execute('SELECT id from users'))
    keys = tuple(row[0] for row in rows)
    return keys


def insert_movie(empty_session):
    empty_session.execute(
        'INSERT INTO movies (title, year, description, runtime, ratings) VALUES ("The Great Wall", 2016, "European '
        'mercenaries searching for black '
        'powder become embroiled in the defense of the Great Wall of China against a horde of monstrous creatures.",'
        '103, 6.1) '
    )

    row = empty_session.execute('SELECT id from movies').fetchone()
    return row[0]


def insert_genre(empty_session):
    empty_session.execute(
        'INSERT INTO genres (name) VALUES ("Sport")'
    )
    row = empty_session.execute('SELECT id FROM genres').fetchone()
    return row[0]


def insert_genres(empty_session):
    empty_session.execute(
        'INSERT INTO genres (name) VALUES ("Sport"), ("Action")'
    )
    rows = list(empty_session.execute('SELECT id from genres'))
    keys = tuple(row[0] for row in rows)
    return keys


def insert_director(empty_session):
    empty_session.execute(
        'INSERT INTO directors (name) VALUES ("David Ayer")'
    )
    row = empty_session.execute('SELECT id FROM directors').fetchone()
    return row[0]


def insert_actor(empty_session):
    empty_session.execute(
        'INSERT INTO actors (name) VALUES ("Will Smith")'
    )
    row = empty_session.execute('SELECT id FROM actors').fetchone()
    return row[0]


def insert_actors(empty_session):
    empty_session.execute(
        'INSERT INTO actors (name) VALUES ("Will Smith"), ("Viola Davis")'
    )
    rows = list(empty_session.execute('SELECT id from actors'))
    keys = tuple(row[0] for row in rows)
    return keys


def test_loading_of_users(empty_session):
    users = list()
    users.append(("Andrew", "1234"))
    users.append(("Cindy", "1111"))
    insert_users(empty_session, users)

    expected = [
        User("Andrew", "1234"),
        User("Cindy", "1111")
    ]
    assert empty_session.query(User).all() == expected


def test_saving_of_users(empty_session):
    user = make_user()
    empty_session.add(user)
    empty_session.commit()

    rows = list(empty_session.execute('SELECT username, password FROM users'))
    assert rows == [("Andrew", "111")]


def test_loading_of_movie(empty_session):
    movie_key = insert_movie(empty_session)
    expected_movie = make_movie()
    fetched_movie = empty_session.query(Movie).one()

    assert expected_movie == fetched_movie
    assert movie_key == fetched_movie.id


def test_loading_of_genre(empty_session):
    insert_genre(empty_session)
    expected_genre = make_genre()
    fetched_genre = empty_session.query(Genre).one()

    assert expected_genre == fetched_genre


def test_loading_of_actor(empty_session):
    insert_actor(empty_session)
    expected_actor = make_actor()
    fetched_actor = empty_session.query(Actor).one()

    assert expected_actor == fetched_actor


def test_loading_of_director(empty_session):
    insert_director(empty_session)
    expected_director = make_director()
    fetched_director = empty_session.query(Director).one()

    assert expected_director == fetched_director


def test_loading_of_tagged_article(empty_session):
    movie_key = insert_movie(empty_session)
    genre_keys = insert_genres(empty_session)
    insert_movie_genre_associations(empty_session, movie_key, genre_keys)

    movie = empty_session.query(Movie).get(movie_key)
    genres = [empty_session.query(Genre).get(key) for key in genre_keys]

    for genre in genres:
        assert movie.is_tagged_by(genre)
        assert genre.is_applied_to(movie)


def test_saving_of_users_with_common_username(empty_session):
    insert_user(empty_session, ("Andrew", "1234"))
    empty_session.commit()

    with pytest.raises(IntegrityError):
        user = User("Andrew", "111")
        empty_session.add(user)
        empty_session.commit()


def insert_movie_genre_associations(empty_session, movie_key, genre_keys):
    stmt = 'INSERT INTO movie_genres (movie_id, genre_id) VALUES (:movie_id, :genre_id)'
    for genre_key in genre_keys:
        empty_session.execute(stmt, {'movie_id': movie_key, 'genre_id': genre_key})


def insert_movie_actor_associations(empty_session, movie_key, actor_keys):
    stmt = 'INSERT INTO movie_actors (movie_id, actor_id) VALUES (:movie_id, :actor_id)'
    for actor_key in actor_keys:
        empty_session.execute(stmt, {'movie_id': movie_key, 'actor_id': actor_key})


def insert_movie_director_associations(empty_session, movie_key, director_key):
    stmt = 'INSERT INTO movie_director (movie_id, director_id) VALUES (:movie_id, :director_id)'
    empty_session.execute(stmt, {'movie_id': movie_key, 'director_id': director_key})


def insert_reviewed_movie(empty_session):
    movie_key = insert_movie(empty_session)
    user_key = insert_user(empty_session)

    timestamp_1 = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    timestamp_2 = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    empty_session.execute(
        'INSERT INTO reviews (user_id, movie_id, review_text, rating, timestamp) VALUES '
        '(:user_id, :movie_id, "Review 1", rating, :timestamp_1),'
        '(:user_id, :movie_id, "Review 2", rating, :timestamp_2)',
        {'user_id': user_key, 'movie_id': movie_key, 'timestamp_1': timestamp_1, 'timestamp_2': timestamp_2}
    )

    row = empty_session.execute('SELECT id from articles').fetchone()
    return row[0]


def test_saving_of_movie(empty_session):
    movie = make_movie()
    empty_session.add(movie)
    empty_session.commit()

    rows = list(empty_session.execute('SELECT title, year, description, runtime, ratings FROM movies'))
    assert rows == [('The Great Wall', 2016, 'European mercenaries searching for black powder become '
                                             'embroiled in the defense of the Great Wall of China against a '
                                             'horde of monstrous creatures.', 103, 6.1)]


def test_saving_starred_movie(empty_session):
    movie = make_movie()
    actor = make_actor()

    # Establish the bidirectional relationship between the Movie and the Genre.
    make_actor_association(movie, actor)

    # Persist the Movie and Genre.
    # Note: it doesn't matter whether we add the Genre or the Movie. They are connected
    # bidirectionally, so persisting either one will persist the other.
    empty_session.add(movie)
    empty_session.commit()

    # Test test_saving_of_movie() checks for insertion into the movies table.
    rows = list(empty_session.execute('SELECT id FROM movies'))
    movie_key = rows[0][0]

    # Check that the genres table has a new record.
    rows = list(empty_session.execute('SELECT id, name FROM actors'))
    actor_key = rows[0][0]
    assert rows[0][1] == "Will Smith"

    # Check that the movie_genres table has a new record.
    rows = list(empty_session.execute('SELECT movie_id, actor_id from movie_actors'))
    movie_foreign_key = rows[0][0]
    actor_foreign_key = rows[0][1]

    assert movie_key == movie_foreign_key
    assert actor_key == actor_foreign_key


def test_saving_dir_movie(empty_session):
    movie = make_movie()
    director = make_director()

    # Establish the bidirectional relationship between the Movie and the Genre.
    make_director_association(movie, director)

    # Persist the Movie and Genre.
    # Note: it doesn't matter whether we add the Genre or the Movie. They are connected
    # bidirectionally, so persisting either one will persist the other.
    empty_session.add(movie)
    empty_session.commit()

    # Test test_saving_of_movie() checks for insertion into the movies table.
    rows = list(empty_session.execute('SELECT id FROM movies'))
    movie_key = rows[0][0]

    # Check that the genres table has a new record.
    rows = list(empty_session.execute('SELECT id, name FROM directors'))
    director_key = rows[0][0]
    print(director_key)
    assert rows[0][1] == "David Ayer"

    # Check that the movie_genres table has a new record.
    rows = list(empty_session.execute('SELECT movie_id, director_id from movie_director'))
    movie_foreign_key = rows[0][0]
    director_foreign_key = rows[0][1]

    assert movie_key == movie_foreign_key
    assert director_key == director_foreign_key


def test_saving_tagged_movie(empty_session):
    movie = make_movie()
    genre = make_genre()

    # Establish the bidirectional relationship between the Movie and the Genre.
    make_genre_association(movie, genre)

    # Persist the Movie and Genre.
    # Note: it doesn't matter whether we add the Genre or the Movie. They are connected
    # bidirectionally, so persisting either one will persist the other.
    empty_session.add(movie)
    empty_session.commit()

    # Test test_saving_of_movie() checks for insertion into the movies table.
    rows = list(empty_session.execute('SELECT id FROM movies'))
    movie_key = rows[0][0]

    # Check that the genres table has a new record.
    rows = list(empty_session.execute('SELECT id, name FROM genres'))
    genre_key = rows[0][0]
    assert rows[0][1] == "Sport"

    # Check that the movie_genres table has a new record.
    rows = list(empty_session.execute('SELECT movie_id, genre_id from movie_genres'))
    movie_foreign_key = rows[0][0]
    genre_foreign_key = rows[0][1]

    assert movie_key == movie_foreign_key
    assert genre_key == genre_foreign_key


def test_save_commented_article(empty_session):
    # Create Article User objects.
    movie = make_movie()
    user = make_user()

    # Create a new Review that is bidirectionally linked with the User and Movie.
    review_text = "Some comment text."
    rating = 8
    review = make_review(user, movie, review_text, rating)

    # Save the new Movie.
    empty_session.add(review)
    empty_session.commit()

    # Test test_saving_of_movie() checks for insertion into the movies table.
    rows = list(empty_session.execute('SELECT id FROM movies'))
    movie_key = rows[0][0]

    # Test test_saving_of_users() checks for insertion into the users table.
    rows = list(empty_session.execute('SELECT id FROM users'))
    user_key = rows[0][0]
    
    # Check that the comments table has a new record that links to the articles and users
    # tables.
    rows = list(empty_session.execute('SELECT user_id, movie_id, review_text, rating FROM reviews'))
    assert rows == [(user_key, movie_key, review_text, rating)]


def test_saving_of_review(empty_session):
    movie_key = insert_movie(empty_session)
    user_key = insert_user(empty_session, ("Andrew", "1234"))

    rows = empty_session.query(Movie).all()
    movie = rows[0]
    user = empty_session.query(User).all()[0]

    # Create a new Comment that is bidirectionally linked with the User and Article.
    review_text = "Some comment text."
    rating = 8
    review = make_review(user, movie, review_text, rating)

    # Note: if the bidirectional links between the new Comment and the User and
    # Article objects hadn't been established in memory, they would exist following
    # committing the addition of the Comment to the database.
    empty_session.add(review)
    empty_session.commit()

    rows = list(empty_session.execute('SELECT user_id, movie_id, review_text, rating FROM reviews'))

    assert rows == [(user_key, movie_key, review_text, rating)]
