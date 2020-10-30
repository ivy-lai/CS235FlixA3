from flask import Blueprint, render_template

import cs235flix.utilities.utilities as utilities
import cs235flix.movies.services as services
import cs235flix.adapters.repository as repo

home_blueprint = Blueprint('home_bp', __name__)


@home_blueprint.route('/', methods=['GET'])
def home():
    return render_template(
        'home/home.html',
        dark_knight=services.get_movie_by_id(55, repo.repo_instance),
        inception=services.get_movie_by_id(81, repo.repo_instance),
        dangal=services.get_movie_by_id(118, repo.repo_instance),
    )
