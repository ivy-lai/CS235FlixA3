from typing import List

from better_profanity import profanity
from flask_wtf import FlaskForm
from werkzeug.utils import redirect
from wtforms import TextAreaField, HiddenField, SubmitField, RadioField, StringField, SelectField
from wtforms.validators import DataRequired, Length, ValidationError

from flask import Blueprint, session, flash
from flask import request, render_template, url_for

import cs235flix.adapters.repository as repo
import cs235flix.movies.services as services
import cs235flix.utilities.utilities as utilities

# Configure Blueprint.
from cs235flix.authentication.authentication import login_required
import cs235flix.authentication.services as auth
from cs235flix.domainmodel.full_model import Genre, Director, Actor

movies_blueprint = Blueprint('movies_bp', __name__)


@movies_blueprint.route('/movies', methods=['GET'])
def movies_by_batch_number():
    movies_per_page = 100

    # read query parameters
    batch_number = request.args.get('batch_number')

    if batch_number is None:
        # No cursor query parameter, so initialise cursor to start at the beginning.
        batch_number = 1
    else:
        # Convert cursor from string to int.
        batch_number = int(batch_number)

    next_page_url = url_for('movies_bp.movies_by_batch_number', batch_number=batch_number + 1)
    previous_page_url = url_for('movies_bp.movies_by_batch_number', batch_number=batch_number - 1)

    page1 = url_for('movies_bp.movies_by_batch_number', batch_number=1)
    page2 = url_for('movies_bp.movies_by_batch_number', batch_number=2)
    page3 = url_for('movies_bp.movies_by_batch_number', batch_number=3)
    page4 = url_for('movies_bp.movies_by_batch_number', batch_number=4)
    page5 = url_for('movies_bp.movies_by_batch_number', batch_number=5)
    page6 = url_for('movies_bp.movies_by_batch_number', batch_number=6)
    page7 = url_for('movies_bp.movies_by_batch_number', batch_number=7)
    page8 = url_for('movies_bp.movies_by_batch_number', batch_number=8)
    page9 = url_for('movies_bp.movies_by_batch_number', batch_number=9)
    page10 = url_for('movies_bp.movies_by_batch_number', batch_number=10)

    # Retrieve all movies
    movie_list = services.get_all_movies(repo.repo_instance)

    for movie in movie_list:
        movie['url'] = url_for('movies_bp.movie_by_id', id=movie['id'])

    return render_template(
        'movies/all_movies.html',
        movie_list=movie_list[(batch_number - 1) * movies_per_page:(batch_number * movies_per_page)],
        previous_page_url=previous_page_url,
        next_page_url=next_page_url,
        batch_number=batch_number,
        page1=page1,
        page2=page2,
        page3=page3,
        page4=page4,
        page5=page5,
        page6=page6,
        page7=page7,
        page8=page8,
        page9=page9,
        page10=page10,
        genre_urls=utilities.get_genre_urls()
    )


@movies_blueprint.route('/movies_by_genre', methods=['GET'])
def movies_by_genre():
    movies_per_page = 100
    # Read query parameters.
    target_genre = request.args.get('genre')

    if target_genre:
        genre = Genre(target_genre[7:-1])
        movie_list = services.get_movie_for_genre(genre, repo.repo_instance)

        for movie in movie_list:
            movie['url'] = url_for('movies_bp.movie_by_id', id=movie['id'])

        return render_template(
            'movies/filter.html',
            type='Genres',
            movie_list=movie_list,
            filter=target_genre
        )

    else:
        return render_template(
            'movies/filter.html',
            type='Genre',
            urls=utilities.get_genre_urls(),
            filter=None
        )


@movies_blueprint.route('/movies_by_director', methods=['GET'])
def movies_by_director():
    names_per_page = 75
    # Read query parameters.
    target_director = request.args.get('director')
    batch_number = request.args.get('batch_number')

    if batch_number is None:
        # No cursor query parameter, so initialise cursor to start at the beginning.
        batch_number = 1
    else:
        # Convert cursor from string to int.
        batch_number = int(batch_number)

    next_page_url = url_for('movies_bp.movies_by_director', batch_number=batch_number + 1)
    previous_page_url = url_for('movies_bp.movies_by_director', batch_number=batch_number - 1)

    page1 = url_for('movies_bp.movies_by_director', batch_number=1)
    page2 = url_for('movies_bp.movies_by_director', batch_number=2)
    page3 = url_for('movies_bp.movies_by_director', batch_number=3)
    page4 = url_for('movies_bp.movies_by_director', batch_number=4)
    page5 = url_for('movies_bp.movies_by_director', batch_number=5)
    page6 = url_for('movies_bp.movies_by_director', batch_number=6)
    page7 = url_for('movies_bp.movies_by_director', batch_number=7)
    page8 = url_for('movies_bp.movies_by_director', batch_number=8)
    page9 = url_for('movies_bp.movies_by_director', batch_number=9)
    page10 = url_for('movies_bp.movies_by_director', batch_number=10)

    urls = utilities.get_director_urls()
    url_list = list(urls.items())

    if target_director:
        director = Director(target_director[10:-1])
        movie_list = services.get_movie_by_director(director, repo.repo_instance)

        for movie in movie_list:
            movie['url'] = url_for('movies_bp.movie_by_id', id=movie['id'])

        return render_template(
            'movies/dir_filter.html',
            type='Directors',
            movie_list=movie_list,
            urls=utilities.get_director_urls(),
            filter=target_director
        )

    else:
        return render_template(
            'movies/dir_filter.html',
            type='Director',
            url_list=url_list[(batch_number - 1) * names_per_page:(batch_number * names_per_page)],
            previous_page_url=previous_page_url,
            next_page_url=next_page_url,
            batch_number=batch_number,
            page1=page1,
            page2=page2,
            page3=page3,
            page4=page4,
            page5=page5,
            page6=page6,
            page7=page7,
            page8=page8,
            page9=page9,
            page10=page10,
            urls=utilities.get_director_urls(),
            filter=None,
        )


@movies_blueprint.route('/movies_by_actor', methods=['GET'])
def movies_by_actor():
    names_per_page = 200
    # Read query parameters.
    target_actor = request.args.get('actor')
    batch_number = request.args.get('batch_number')
    if batch_number is None:
        # No cursor query parameter, so initialise cursor to start at the beginning.
        batch_number = 1
    else:
        # Convert cursor from string to int.
        batch_number = int(batch_number)

    next_page_url = url_for('movies_bp.movies_by_actor', batch_number=batch_number + 1)
    previous_page_url = url_for('movies_bp.movies_by_actor', batch_number=batch_number - 1)

    page1 = url_for('movies_bp.movies_by_actor', batch_number=1)
    page2 = url_for('movies_bp.movies_by_actor', batch_number=2)
    page3 = url_for('movies_bp.movies_by_actor', batch_number=3)
    page4 = url_for('movies_bp.movies_by_actor', batch_number=4)
    page5 = url_for('movies_bp.movies_by_actor', batch_number=5)
    page6 = url_for('movies_bp.movies_by_actor', batch_number=6)
    page7 = url_for('movies_bp.movies_by_actor', batch_number=7)
    page8 = url_for('movies_bp.movies_by_actor', batch_number=8)
    page9 = url_for('movies_bp.movies_by_actor', batch_number=9)
    page10 = url_for('movies_bp.movies_by_actor', batch_number=10)

    urls = utilities.get_actor_urls()
    url_list = list(urls.items())

    if target_actor:
        actor = Actor(target_actor[7:-1])
        movie_list = services.get_movie_for_actor(actor, repo.repo_instance)

        for movie in movie_list:
            movie['url'] = url_for('movies_bp.movie_by_id', id=movie['id'])

        return render_template(
            'movies/dir_filter.html',
            type='Actors',
            movie_list=movie_list,
            filter=target_actor
        )

    else:

        return render_template(
            'movies/dir_filter.html',
            type='Actor',
            urls=utilities.get_actor_urls(),
            url_list=url_list[(batch_number - 1) * names_per_page:(batch_number * names_per_page)],
            previous_page_url=previous_page_url,
            next_page_url=next_page_url,
            batch_number=batch_number,
            page1=page1,
            page2=page2,
            page3=page3,
            page4=page4,
            page5=page5,
            page6=page6,
            page7=page7,
            page8=page8,
            page9=page9,
            page10=page10,
            filter=None,
        )


@movies_blueprint.route('/movie_by_id', methods=['GET'])
def movie_by_id():
    # Read query parameters.
    target_id = int(request.args.get('id'))
    # movie_to_show_reviews = request.args.get('view_reviews_for')

    # Fetch the first and last movie in the series.
    # first_movie = services.get_first_movie(repo.repo_instance)
    # last_movie = services.get_last_movie(repo.repo_instance)

    if target_id is not None:
        movie = services.get_movie_by_id(target_id, repo.repo_instance)
        watchlist=services.get_watchlist(repo.repo_instance)
        if len(movie) > 0:
            watch=0
            for item in watchlist:
                if movie['id'] == item['id']:
                   watch += 1
            # Construct urls for viewing movie comments and adding comments.
            # movie['view_comment_url'] = url_for('movies_bp.movie_by_id', id=target_id, view_reviews_for=movie['id'])
            movie['add_review_url'] = url_for('movies_bp.review_on_movie', movie=movie['id'])
            movie['add_watchlist_url'] = url_for('movies_bp.movies_by_watchlist', movie=movie['id'])
            movie['delete_watchlist_url'] = url_for('movies_bp.delete_watchlist', movie=movie['id'])
            # Generate the webpage to display the movie.
            return render_template(
                'movies/one_movie.html',
                title='Movie',
                movie=movie,
                watch=watch, watchlist=watchlist
            )


@movies_blueprint.route('/watchlist', methods=['GET'])
@login_required
def movies_by_watchlist():
    movie_id = int(request.args.get('movie'))
    movies = services.get_movie_by_id(movie_id, repo.repo_instance)
    services.add_watchlist(movies['id'], repo.repo_instance)
    watchlist = services.get_watchlist(repo.repo_instance)
    return render_template(
        'movies/watchlist.html',
        title='Added to',
        watchlist=watchlist,
        view=url_for('movies_bp.view_watchlist')
    )


@movies_blueprint.route('/remove', methods=['GET'])
@login_required
def delete_watchlist():
    movie_id = int(request.args.get('movie'))
    print(movie_id)
    movies = services.get_movie_by_id(movie_id, repo.repo_instance)
    services.remove_watchlist(movies['id'], repo.repo_instance)
    watchlist = services.get_watchlist(repo.repo_instance)
    return render_template(
        'movies/watchlist.html',
        title='Removed from',
        watchlist=watchlist,
        view=url_for('movies_bp.view_watchlist')
    )


@movies_blueprint.route('/view_watchlist', methods=['GET'])
@login_required
def view_watchlist():
    watchlist = list()
    movies = services.get_watchlist(repo.repo_instance)
    if len(movies) > 0:
        for movie in movies:
            movie['url'] = url_for('movies_bp.movie_by_id', id=movie['id'])
            if movie not in watchlist:
                watchlist.append(movie)

    return render_template(
        'movies/view_watchlist.html',
        title='Watchlist',
        watchlist=watchlist
    )


@movies_blueprint.route('/review', methods=['GET', 'POST'])
@login_required
def review_on_movie():
    # Obtain the username of the currently logged in user.
    username = session['username']

    form = ReviewForm()

    if form.validate_on_submit():
        movie_id = int(form.movie_id.data)
        services.add_review(username, movie_id, form.comment.data, int(form.rating.data[0]), repo.repo_instance)

        # Retrieve the movie in dict form.
        movie_dict = services.get_movie_by_id(movie_id, repo.repo_instance)
        return redirect(url_for('movies_bp.movie_by_id', id=movie_dict['id']))

    if request.method == 'GET':
        # Request is a HTTP GET to display the form.
        movie_id = request.args.get('movie')
    #     # Store the movie id in the form.
        form.movie_id.data = int(movie_id)
    else:
        # Request is a HTTP POST where form validation has failed.
        # Extract the movie id of the movie being commented from the form.
        movie_id = int(form.movie_id.data)

    # For a GET or an unsuccessful POST, retrieve the movie to comment in dict form, and return a Web page that allows
    # the user to enter a comment. The generated Web page includes a form object.
    movie = services.get_movie_by_id(int(movie_id), repo.repo_instance)
    return render_template(
        'movies/review_on_movie.html',
        title='Edit movie',
        movie=movie,
        form=form,
        handler_url=url_for('movies_bp.review_on_movie'),
        tag_urls=utilities.get_genre_urls()
    )


@movies_blueprint.route('/search', methods=['GET', 'POST'])
def search():
    search_string = request.args.get('search')
    select = request.args.get('select')

    actor_results = list()
    director_results = list()
    genre_results = list()
    title_results = list()
    all_results = list()

    if search_string == '':
        return render_template('movies/results.html',
                               results=all_results)
    else:
        all_movies = services.get_all_movies(repo.repo_instance)
        for movie in all_movies:
            if select == 'Titles' or select == 'All':
                if search_string.lower() in movie['title'].lower():
                    title_results.append(movie)
            if select == 'Genres' or select == 'All':
                for genre in movie['genres']:
                    if search_string.lower() in genre['name'].lower():
                        genre_results.append(movie)
            if select == 'Actors' or select == 'All':
                for actor in movie['actors']:
                    if search_string.lower() in actor['name'].lower():
                        actor_results.append(movie)
            if select == 'Directors' or select == 'All':
                if search_string.lower() in movie['director']['name'].lower():
                    director_results.append(movie)

        for item in actor_results:
            if item not in all_results:
                all_results.append(item)
        for item in genre_results:
            if item not in all_results:
                all_results.append(item)
        for item in title_results:
            if item not in all_results:
                all_results.append(item)
        for item in director_results:
            if item not in all_results:
                all_results.append(item)
        return render_template('movies/results.html',
                               results=all_results)


class ProfanityFree:
    def __init__(self, message=None):
        if not message:
            message = u'Field must not contain profanity'
        self.message = message

    def __call__(self, form, field):
        if profanity.contains_profanity(field.data):
            raise ValidationError(self.message)


class ReviewForm(FlaskForm):
    comment = TextAreaField('Comment', [
        DataRequired(),
        Length(min=4, message='Your comment is too short'),
        ProfanityFree(message='Your comment must not contain profanity')])
    rating = SelectField('Rating', choices=[('1 Star', 1), ('2 Stars', 2), ('3 Stars', 3), ('4 Stars', 4), ('5 Stars', 5), ('6 Stars', 6), ('7 Stars', 7), ('8 Stars', 8), ('9 Stars', 9), ('10 Stars', 10)])
    movie_id = HiddenField("Movie id")
    submit = SubmitField('Submit')


class SearchForm(FlaskForm):
    choices = [('All', 'All'),
               ('Actor', 'Actor'),
               ('Director', 'Director'),
               ('Genre', 'Genre'),
               ('Title', 'Title')]
    select = SelectField('Search for movie:', choices=choices)
    search = StringField('')
