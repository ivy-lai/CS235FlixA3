from datetime import datetime
from typing import List, Iterable, Iterator
import random


class User:
    def __init__(self, username: str, password: str):
        self.__username = username
        self.__password = password
        self.__watched_movies = []
        self.__reviews = []
        self.__time_spent_watching_movies_minutes = 0

    def __repr__(self):
        return f"<User {self.__username}>"

    def __eq__(self, other):
        return self.__username == other.__username

    def __lt__(self, other):
        return self.__username < other.__user_name

    def __hash__(self):
        return hash((self.__username, self.__password))

    def watch_movie(self, movie):
        self.__watched_movies.append(movie)
        self.__time_spent_watching_movies_minutes += int(movie.runtime_minutes)

    def add_review(self, review):
        self.__reviews.append(review)

    @property
    def username(self):
        return self.__username

    @property
    def password(self):
        return self.__password

    @property
    def watched_movies(self):
        return self.__watched_movies

    @property
    def reviews(self):
        return iter(self.__reviews)

    @property
    def time_spent_watching_movies_minutes(self):
        return self.__time_spent_watching_movies_minutes


class Review:

    def __init__(self, user, movie, review_text, rating, timestamp: datetime):
        if type(rating) == int and 1 <= rating <= 10:
            self.__rating = rating
        else:
            self.__rating = None
        self.__user = user
        self.__timestamp: datetime = timestamp
        self.__movie = movie
        self.__review_text = review_text

    @property
    def user(self) -> User:
        return self.__user

    @property
    def movie(self):
        return self.__movie

    @property
    def review_text(self):
        return self.__review_text

    @property
    def rating(self):
        return self.__rating

    @property
    def timestamp(self) -> datetime:
        return self.__timestamp

    def __repr__(self):
        return f"<Review for {self.__movie}: {self.__rating}, {self.__review_text}>"

    def __eq__(self, other):
        if isinstance(other, Review):
            return (self.__movie, self.__review_text, self.__rating, self.__timestamp) == (
                other.__movie, other.__review_text, other.__rating, other.__timestamp)
        else:
            return False


class Movie:

    def __set_title_internal(self, title: str):
        if title is None or title.strip() == "" or type(title) is not str:
            self.__title = None
        else:
            self.__title = title.strip()

    def __set_year_internal(self, release_year: int):
        if release_year is None or release_year < 1900 or type(release_year) is not int:
            self.__year = None
        else:
            self.__year = release_year

    def __init__(self, id: int, title: str, release_year: int):
        self.__id: int = id
        self.__set_title_internal(title)
        self.__set_year_internal(release_year)

        self.__description = None
        self.__director = None
        self.__actors = []
        self.__genres = []
        self.__runtime = None
        self.__reviews = []
        self.__rating = []

    def __init__(self, id: int, title: str, release_year: int, description: str, runtime: int, rating: float):
        self.__id: int = id
        self.__set_title_internal(title)
        self.__set_year_internal(release_year)
        self.__genres: List[Genre] = list()
        self.__ratings = rating

        if type(description) == str and description != "":
            self.__description = description
        else:
            self.__description = None

        if type(runtime) == int and runtime >= 1:
            self.__runtime = runtime
        else:
            self.__runtime = None

        self.__director = None
        self.__actors = []
        self.__genres = []
        self.__reviews = []

    @property
    def id(self) -> int:
        return self.__id

    @property
    def title(self) -> str:
        return self.__title

    @title.setter
    def title(self, title: str):
        self.__set_title_internal(title)

    @property
    def year(self) -> int:
        return self.__year

    @year.setter
    def year(self, release_year: int):
        self.__set_year_internal(release_year)

    # additional attributes

    @property
    def description(self) -> str:
        return self.__description

    @description.setter
    def description(self, description: str):
        if type(description) is str:
            self.__description = description.strip()
        else:
            self.__description = None

    @property
    def director(self) -> 'Director':
        return self.__director

    def add_director(self, director: 'Director'):
        if isinstance(director, Director):
            self.__director = director
        else:
            self.__director = None

    @property
    def actors(self) -> Iterator:
        actors_list = []
        for actors in self.__actors:
            if actors not in actors_list:
                actors_list.append(actors)
        return actors_list
        #return iter(self.__actors)

    def add_actor(self, actor: 'Actor'):
        if not isinstance(actor, Actor) or actor in self.__actors:
            return
        self.__actors.append(actor)

    def remove_actor(self, actor: 'Actor'):
        if not isinstance(actor, Actor):
            return

        try:
            self.__actors.remove(actor)
        except ValueError:
            # print(f"Movie.remove_actor: Could not find {actor} in list of actors.")
            pass

    @property
    def genres(self) -> Iterable['Genre']:
        genres_list = []
        for genre in self.__genres:
            if genre not in genres_list:
                genres_list.append(genre)
        return genres_list
        # return iter(self.__genres)

    def add_genre(self, genre: 'Genre'):
        if not isinstance(genre, Genre) or genre in self.__genres:
            return

        self.__genres.append(genre)

    def remove_genre(self, genre: 'Genre'):
        if not isinstance(genre, Genre):
            return

        try:
            self.__genres.remove(genre)
        except ValueError:
            # print(f"Movie.remove_genre: Could not find {genre} in list of genres.")
            pass

    def is_tagged_by(self, genre: 'Genre'):
        return genre in self.__genres

    def is_tagged(self) -> bool:
        return len(self.__genres) > 0

    def is_actor_in(self, actor: 'Actor'):
        return actor in self.__actors

    def is_actor(self) -> bool:
        return len(self.__actors) > 0

    @property
    def runtime(self) -> int:
        return self.__runtime

    @runtime.setter
    def runtime(self, val: int):
        if val > 0:
            self.__runtime = val
        else:
            raise ValueError(f'Movie.runtime_minutes setter: Value out of range {val}')

    @property
    def ratings(self) -> int:
        return self.__ratings

    @ratings.setter
    def ratings(self, val: int):
        if 0 < val < 10:
            self.__ratings = val
        else:
            raise ValueError(f'Movie.rating setter: Value out of range {val}')

    @property
    def number_of_reviews(self) -> int:
        return len(self.__reviews)

    @property
    def reviews(self) -> Iterable['Review']:
        reviews_list = []
        for review in self.__reviews:
            if review not in reviews_list:
                reviews_list.append(review)
        return reviews_list

    def add_review(self, review: Review):
        self.__reviews.append(review)

    @property
    def number_of_genres(self) -> int:
        return len(self.__genres)

    def __get_unique_string_rep(self):
        return f"{self.__title}, {self.__year}"

    def __repr__(self):
        return f'<Movie {self.__get_unique_string_rep()}>'

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.__get_unique_string_rep() == other.__get_unique_string_rep()

    def __lt__(self, other):
        if other.title is not None and other.__year is not None:
            return (self.__title, self.__year) < (other.__title, other.__year)

    def __hash__(self):
        return hash(self.__get_unique_string_rep())


class Genre:
    def __init__(self, genre_name: str):
        if genre_name == "" or type(genre_name) is not str:
            self.__genre_name = None
        else:
            self.__genre_name = genre_name.strip()
        self.__tagged_movies: List[Movie] = list()

    @property
    def name(self) -> str:
        return self.__genre_name

    @property
    def tagged_movies(self) -> Iterable[Movie]:
        return iter(self.__tagged_movies)

    @property
    def number_of_tagged_movies(self) -> int:
        return len(self.__tagged_movies)

    def is_applied_to(self, movie: Movie) -> bool:
        return movie in self.__tagged_movies

    def add_movie(self, movie: Movie):
        self.__tagged_movies.append(movie)

    def __repr__(self):
        return f"<Genre {self.__genre_name}>"

    def __eq__(self, other):
        if isinstance(other, Genre):
            return self.__genre_name == other.__genre_name
        else:
            return False

    def __lt__(self, other):
        return self.__genre_name < other.__genre_name

    def __hash__(self):
        return hash(self.__genre_name)


class Actor:
    def __init__(self, actor_full_name: str):
        if actor_full_name == "" or type(actor_full_name) is not str:
            self.__actor_full_name = None
        else:
            self.__actor_full_name = actor_full_name.strip()
        self.__colleagueList = []
        self.__starred_movies = []

    @property
    def name(self) -> str:
        return self.__actor_full_name

    @property
    def starred_movies(self) -> Iterable[Movie]:
        return iter(self.__starred_movies)

    @property
    def number_of_starred_movies(self) -> int:
        return len(self.__starred_movies)

    def __repr__(self):
        return f"<Actor {self.__actor_full_name}>"

    def __eq__(self, other):
        if isinstance(other, Actor):
            return self.__actor_full_name == other.__actor_full_name
        else:
            return False

    def __lt__(self, other):
        return self.__actor_full_name < other.__actor_full_name

    def __hash__(self):
        return hash(self.__actor_full_name)

    def add_actor_colleague(self, colleague):
        self.__colleagueList += [colleague]

    def add_movie(self, movie):
        self.__starred_movies.append(movie)

    def is_applied_to(self, movie: Movie) -> bool:
        return movie in self.__starred_movies

    def check_if_this_actor_worked_with(self, colleague):
        if colleague in self.__colleagueList:
            return True
        else:
            return False


class Director:

    def __init__(self, director_full_name: str):
        if director_full_name == "" or type(director_full_name) is not str:
            self.__director = None
        else:
            self.__director = director_full_name.strip()
        self.__dir_movies = list()

    @property
    def name(self) -> str:
        return self.__director

    @property
    def director_movies(self) -> Iterable[Movie]:
        return iter(self.__dir_movies)

    def is_applied_to(self, movie: Movie) -> bool:
        return movie in self.__dir_movies

    def __repr__(self):
        return f"<Director {self.__director}>"

    def __eq__(self, other):
        if isinstance(other, Director):
            return self.__director == other.__director
        else:
            return False

    def __lt__(self, other):
        return self.__director < other.__director

    def __hash__(self):
        return hash(self.__director)

    def add_movie(self, movie):
        self.__dir_movies.append(movie)


class WatchList:
    def __init__(self):
        self.__watchList = []
        self.__genre_list = []

    def add_movie(self, movie):
        if movie not in self.__watchList:
            self.__watchList.append(movie)

    def remove_movie(self, movie):
        if movie in self.__watchList:
            self.__watchList.remove(movie)

    def select_movie_to_watch(self, index):
        if index >= len(self.__watchList):
            return None
        else:
            return self.__watchList[index]

    def size(self):
        return len(self.__watchList)

    def first_movie_in_watchlist(self):
        if len(self.__watchList) == 0:
            return None
        else:
            return self.__watchList[0]

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.__watchList) == 0:
            raise StopIteration
        else:
            return self.__watchList.pop(0)

    def choose_random(self):
        n = random.randint(0, len(self.__watchList))
        return self.__watchList[n - 1]


class ModelException(Exception):
    pass


def make_review(user: User, movie: Movie, review_text: str, rating: int, timestamp: datetime = datetime.today()):
    review = Review(user, movie, review_text, rating, timestamp)
    user.add_review(review)
    movie.add_review(review)

    return review


def make_genre_association(movie: Movie, genre: Genre):
    if genre.is_applied_to(movie):
        raise ModelException(f'Genre {genre.name} already applied to Movie "{movie.title}"')

    movie.add_genre(genre)
    genre.add_movie(movie)


def make_director_association(movie: Movie, director: Director):
    if director.is_applied_to(movie):
        raise ModelException(f'Director {director.name} already applied to Movie "{movie.title}"')

    movie.add_director(director)
    director.add_movie(movie)


def make_actor_association(movie: Movie, actor: Actor):
    if actor.is_applied_to(movie):
        raise ModelException(f'Actor {actor.name} already applied to Movie "{movie.title}"')

    movie.add_actor(actor)
    actor.add_movie(movie)