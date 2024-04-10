#! /usr/bin/env python
import csv
from mrjob.job import MRJob


class MRMoviesByGenreCount(MRJob):
    """
    Find the distinct number of movies in specific genres over time
    """

    def mapper(self, _, line):
        """
        Implement your mapper here!

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) 
                key: str
                value: int
                You'll have to design your format for the (key, value) pairs so that
                your reducer does the result operations correctly.

        """
        # yield key, value pairs for your program
        reader = csv.reader([line])
        for word in reader:
            title_year, genre = word[0], word[1]
            if genre=="Western" or genre=="Sci-Fi":
                year = title_year[-5:-1]
                if year.isdigit():
                    title = title_year[:-6]
                    year_genre = year+" "+genre
                    yield ((year, genre), title)


    # optional: implement the combiner:
    # def combiner(self, key, values):
        # start using the key-value pairs to calculate the query result
        # pass

    def reducer(self, key, values):
        """
        Implement your reducer here!

        You'll have to use the (key, list(values)) pairs that you designed
        to calculate the end result and output a (key, result) pair.
        
        Parameters:
            key: str
                same as the key defined in the mapper
            values: list
                list containing values corresponding to the key defined in the mapper

            Yields:
                key: str
                    same as the key defined in the mapper
                value: int
                    value corresponding to each key.
        """
        # use the key-value pairs to calculate the query result
    
        yield (key, len(set(values)))

# don't forget the '__name__' == '__main__' clause!
if __name__ == '__main__':
    MRMoviesByGenreCount.run()
