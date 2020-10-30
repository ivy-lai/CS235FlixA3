import csv
import os

from bisect import bisect_left, insort_left

from cs235flix.adapters.repository import AbstractRepository
from cs235flix.domainmodel.full_model import Movie, Actor, Director, Genre, User, make_genre_association, \
    make_director_association, make_actor_association, Review


class MemoryRepository(AbstractRepository):

    def __init__(self):
        self._titles = list()
        self._actors = list()
        self._directors = list()
        self._genres = list()
        self._users = list()
        self._reviews = list()
        self._watchList = list()
        self._movies = list()
        self._movies_index = dict()
        self._ratings = list()

    def get_all_user(self):
        return self._users

    def get_ratings(self):
        return self._ratings

    def add_genre(self, rating):
        self._ratings.append(rating)

    def get_ids(self):
        ids = list()
        for movie in self._movies:
            ids.append(movie.id)
        return ids

    def remove_watchlist(self, movie):
        if movie in self._watchList:
            self._watchList.remove(movie)

    def get_genres(self):
        return self._genres

    def add_genre(self, genre_name):
        self._genres.append(genre_name)

    def get_ratings(self):
        return self._ratings

    def add_ratings(self, rating):
        self._ratings.append(rating)

    def get_directors(self):
        return self._directors

    def add_director(self, director):
        self._directors.append(director)

    def get_actors(self):
        return self._actors

    def add_actor(self, actor_name):
        self._actors.append(actor_name)

    def add_user(self, user: User):
        self._users.append(user)

    def get_user(self, username) -> User:
        return next((user for user in self._users if user.username == username), None)

    def get_watchlist(self):
        return self._watchList

    def add_watchlist(self, movie):
        self._watchList.append(movie)

    def add_movie(self, movie: Movie):
        self._movies.append(movie)
        self._movies_index[movie.id] = movie

    def get_movie(self, title: str, year: int) -> Movie:
        movie = None
        for item in self._movies:
            if item.title == title and item.year == year:
                movie = item

        return movie

    def get_movie_by_id(self, id: int) -> Movie:
        movie = None
        try:
            movie = self._movies_index[id]
        except KeyError:
            pass  # Ignore exception and return None.

        return movie

    def get_movies_by_id(self, id_list):
        existing_ids = [id for id in id_list if id in self._movies_index]

        # Fetch the Articles.
        movies = [self._movies_index[id] for id in existing_ids]
        return movies

    def get_movie_for_genre(self, genre_name: str):
        movie_list = list()
        # Retrieve the ids of articles associated with the Genre.
        if genre_name is not None:
            for movie in self._movies:
                for genre in movie.genres:
                    if genre == genre_name:
                        movie_list.append(movie)
        return movie_list

    def get_movie_id_by_genre(self, genre_name: str):
        # Linear search, to find the first occurrence of a Tag with the name tag_name.
        genre = next((genre for genre in self._genres if genre.name == genre_name), None)

        # Retrieve the ids of articles associated with the Tag.
        if genre is not None:
            movie_ids = [movie.id for movie in genre.tagged_movies]
        else:
            # No Tag with name tag_name, so return an empty list.
            movie_ids = list()

        return movie_ids

    def get_movie_by_director(self, director_name: str):
        movie_list = list()
        # Retrieve the ids of articles associated with the Genre.
        if director_name is not None:
            for movie in self._movies:
                if movie.director == director_name:
                    movie_list.append(movie)
        return movie_list

    def get_movie_for_actor(self, actor_name: str):
        movie_list = list()
        if actor_name is not None:
            for movie in self._movies:
                for actor in movie.actors:
                    if actor == actor_name:
                        movie_list.append(movie)
        return movie_list

    def get_number_of_movies(self):
        return len(self._movies)

    def get_all_movies(self):
        # Fetch the Movies.
        movies = list()
        for movie in self._movies:
            movies.append(movie)
        return movies

    def add_review(self, review: Review):
        # super().add_review(review)
        self._reviews.append(review)

    def get_reviews(self):
        return self._reviews

    # Helper method to return movie index.
    def movies_index(self, movie: Movie):
        index = bisect_left(self._movies, movie)
        if index != len(self._movies) and self._movies[index].genres == movie.genres:
            return index
        raise ValueError


def read_csv_file(filename: str):
    with open(filename) as infile:
        reader = csv.reader(infile)

        # Read first line of the the CSV file.
        headers = next(reader)

        # Read remaining rows from the CSV file.
        for row in reader:
            # Strip any leading/trailing white space from data read.
            row = [item.strip() for item in row]
            yield row


def load_movies(data_path: str, repo: MemoryRepository):
    genres = dict()
    actors = dict()
    for data_row in read_csv_file(os.path.join(data_path, 'Data1000Movies.csv')):
        movie_id = data_row[0]
        movie_genres = data_row[2].split(',')
        movie_actors = data_row[5].split(',')

        for genre in movie_genres:
            if genre not in genres.keys():
                genres[genre] = list()
            genres[genre].append(movie_id)

        for actor in movie_actors:
            if actor not in actors.keys():
                actors[actor] = list()
            actors[actor].append(movie_id)

        # Create Movie Object
        movie = Movie(
            int(data_row[0]),
            data_row[1],
            int(data_row[6]),
            description=data_row[3],
            runtime=int(data_row[7]),
            rating=float(data_row[8])
        )
        repo.add_movie(movie)

        director = Director(data_row[4])
        make_director_association(repo.get_movie_by_id(int(movie_id)), director)
        repo.add_director(director)

    for genre_name in genres.keys():
        genre = Genre(genre_name)
        for movie_id in genres[genre_name]:
            movie = repo.get_movie_by_id(int(movie_id))
            make_genre_association(movie, genre)
        repo.add_genre(genre)

    for actor_name in actors.keys():
        actor = Actor(actor_name)
        for movie_id in actors[actor_name]:
            movie = repo.get_movie_by_id(int(movie_id))
            make_actor_association(movie, actor)
        repo.add_actor(actor)


def populate(data_path: str, repo: MemoryRepository):
    # Load comments into the repository.
    load_movies(data_path, repo)
