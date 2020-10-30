from cs235flix.datafilereaders.movie_file_csv_reader import MovieFileCSVReader


def main():
    filename = 'cs235flix/datafiles/Data1000Movies.csv'
    movie_file_reader = MovieFileCSVReader(filename)
    movie_file_reader.read_csv_file()


if __name__ == "__main__":
    main()