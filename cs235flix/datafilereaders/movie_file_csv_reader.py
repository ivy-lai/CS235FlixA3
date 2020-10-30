import csv

from cs235flix.domainmodel.full_model import Movie
from cs235flix.domainmodel.full_model import Genre
from cs235flix.domainmodel.full_model import Actor
from cs235flix.domainmodel.full_model import Director


class MovieFileCSVReader:

    def __init__(self, file_name: str):
        self.__file_name = file_name
        self.__dataset_of_movies = []
        self.__dataset_of_actors = []
        self.__dataset_of_directors = []
        self.__dataset_of_genres = []
        self.__file_name = file_name
        self.__genre_request = []
        self.__g_request = None
        self.__actor_request = []
        self.__a_request = None

    def read_csv_file(self):
        with open(self.__file_name, mode='r', encoding='utf-8-sig') as csvfile:
            movie_file_reader = csv.DictReader(csvfile)

            index = 0
            for row in movie_file_reader:
                title = row['Title']
                release_year = int(row['Year'])
                #print(f"Movie {index+1} with title: {title}, release year {release_year}")
                index += 1

                movie = Movie(title, release_year)
                if movie not in self.__dataset_of_movies:
                    self.__dataset_of_movies.append(movie)

                actors = row['Actors']
                actor = actors.split(',')
                for item in actor:
                    a = Actor(item.strip())
                    if a not in self.__dataset_of_actors:
                        self.__dataset_of_actors.append(a)

                directors = row['Director']
                d = Director(directors)
                if d not in self.__dataset_of_directors:
                    self.__dataset_of_directors.append(d)

                genres = row['Genre']
                genre = genres.split(',')
                for item in genre:
                    g = Genre(item.strip())
                    if g not in self.__dataset_of_genres:
                        self.__dataset_of_genres.append(g)

    def genre_input(self):
        self.__g_request = Genre(input("Enter genre name to view movies: "))
        if self.__g_request.__eq__(Genre(None)):
            print("No genre entered")
        if self.__g_request in self.__dataset_of_genres:
            with open(self.__file_name, mode='r', encoding='utf-8-sig') as csvfile:
                movie_file_reader = csv.DictReader(csvfile)

                index = 0
                for row in movie_file_reader:
                    title = row['Title']
                    release_year = int(row['Year'])
                    genres = row['Genre']
                    genre = genres.split(',')
                    index += 1

                    movie = Movie(title, release_year)
                    for item in genre:
                        g = Genre(item.strip())
                        if g.__eq__(self.__g_request):
                            self.__genre_request.append(movie)

        else:
            print("Genre not found")

    def actor_input(self):
        self.__a_request = Actor(input("Enter actor name to view movies: "))
        if self.__a_request.__eq__(Actor(None)):
            print("No actor entered")
        if self.__a_request in self.__dataset_of_actors:
            with open(self.__file_name, mode='r', encoding='utf-8-sig') as csvfile:
                movie_file_reader = csv.DictReader(csvfile)

                index = 0
                for row in movie_file_reader:
                    title = row['Title']
                    release_year = int(row['Year'])
                    actors = row['Actors']
                    actor = actors.split(',')
                    index += 1
                    movie = Movie(title, release_year)

                    for item in actor:
                        a = Actor(item.strip())
                        if a.__eq__(self.__a_request):
                            self.__actor_request.append(movie)
        else:
            print("Actor not found")

    @property
    def genre_request(self):
        return self.__genre_request

    @genre_request.setter
    def genre_request(self, movie):
        self.__genre_request = movie

    @property
    def actor_request(self):
        return self.__actor_request

    @actor_request.setter
    def actor_request(self, movie):
        self.__actor_request = movie

    @property
    def dataset_of_movies(self):
        return self.__dataset_of_movies

    @dataset_of_movies.setter
    def dataset_of_movies(self, movies):
        if type(movies) == Movie:
            self.__dataset_of_movies = movies

    @property
    def dataset_of_actors(self):
        return self.__dataset_of_actors

    @dataset_of_actors.setter
    def dataset_of_actors(self, actors):
        if type(actors) == Actor:
            self.__dataset_of_actors = actors

    @property
    def dataset_of_directors(self):
        return self.__dataset_of_directors

    @dataset_of_directors.setter
    def dataset_of_directors(self, directors):
        if type(directors) == Director:
            self.__dataset_of_directors = directors

    @property
    def dataset_of_genres(self):
        return self.__dataset_of_genres

    @dataset_of_genres.setter
    def dataset_of_genres(self, genres):
        if type(genres) == Genre:
            self.__dataset_of_genres = genres
