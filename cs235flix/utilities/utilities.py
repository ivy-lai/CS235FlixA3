from flask import Blueprint, url_for

import cs235flix.adapters.repository as repo
import cs235flix.utilities.services as services


# Configure Blueprint.
utilities_blueprint = Blueprint(
    'utilities_bp', __name__)


def get_movie_urls():
    id_list = services.get_ids(repo.repo_instance)
    movie_urls = dict()
    for id in id_list:
        movie_urls[id] = url_for('movies_bp.movie_by_id', id=id)
    return dict(sorted(movie_urls.items()))


def get_genre_urls():
    genre_names = services.get_genre(repo.repo_instance)
    genre_urls = dict()
    for genre_name in genre_names:
        genre_urls[genre_name] = url_for('movies_bp.movies_by_genre', genre=genre_name)
    return dict(sorted(genre_urls.items()))


def get_director_urls():
    director_names = services.get_directors(repo.repo_instance)
    director_urls = dict()
    for director_name in director_names:
        director_urls[director_name] = url_for('movies_bp.movies_by_director', director=director_name)
    return dict(sorted(director_urls.items()))


def get_actor_urls():
    actor_names = services.get_actors(repo.repo_instance)
    actor_urls = dict()
    for actor_name in actor_names:
        actor_urls[actor_name] = url_for('movies_bp.movies_by_actor', actor=actor_name)
    return dict(sorted(actor_urls.items()))


def get_selected_movie(quantity=25):
    movies = services.get_random_movie(25, repo.repo_instance)
    genre_names = services.get_genre(repo.repo_instance)

    for movie in movies:
        movie['hyperlink'] = url_for('movies_bp.movies_by_genre', genre=genre_names)
    return movies


