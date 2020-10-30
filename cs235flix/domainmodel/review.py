from datetime import datetime
from cs235flix.domainmodel.full_model import Movie


class Review:

    def __init__(self, movie, review_text, rating):
        if type(rating) == int and 1 <= rating <= 10:
            self.__rating = rating
        else:
            self.__rating = None
        self.__datetime = datetime.date(datetime.now())
        self.__movie = movie
        self.__review_text = review_text

    @property
    def movie(self):
        return self.__movie

    @property
    def review_text(self):
        return self.__review_text

    @property
    def rating(self):
        return self.__rating

    def __repr__(self):
        return f"<Review for {self.__movie}: {self.__rating}, {self.__review_text}>"

    def __eq__(self, other):
        if isinstance(other, Review):
            return (self.__movie, self.__review_text, self.__rating, self.__datetime) == (other.__movie, other.__review_text, other.__rating, other.__datetime)
        else:
            return False