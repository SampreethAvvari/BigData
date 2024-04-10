#! /usr/bin/env python

from mrjob.job import MRJob
import re
import string

WORD_RE = re.compile(r"[\S]+")


class MRWordFreqCount(MRJob):
    """
    A class to count word frequency in an input file.
    """

    def mapper(self, _, line):
        """
        This is a mapper function: it finds words in each line of input,
        and yields (key, value) pairs of the form (word, 1), so that the
        reducer can sum the number of words later.
        
        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) pairs of the form where key is word and value is 1
        """
        for word in WORD_RE.findall(line):
            # strip any punctuation
            word = word.strip(string.punctuation)

            # enforce lowercase
            word = word.lower()

            yield (word, 1)

    # optional, but nice to have
    def combiner(self, word, counts):
        """
        This is an example combiner. It has the same code as the reducer
        since this operation is commutative and associative.
        
        Parameters:
            word: str
                single word
            counts: list
                list containing 1s representing each occurances of the word

            Yields:
                word: str
                    single word as key
                sum_words: int
                    number of occurances for the word as value(sum of list containing 1s representing each occurances of the word)
        """
        yield (word, sum(counts))

    def reducer(self, word, counts):
        """
        The reducer takes (key, list(values)) as input, and returns a single
        (key, result) pair. This is specific to `mrjob`, and isn't usually
        required by Hadoop.

        This function just runs a sum across the list of the values (which are
        all 1), returning the word as the key and the number of occurrences
        as the value.
        
        Parameters:
            word: str
                single word
            counts: list
                list containing 1s representing each occurances of the word

            Yields:
                word: str
                    single word as key
                sum_words: int
                    number of occurances for the word as value(sum of list containing 1s representing each occurances of the word)
        """
        yield (word, sum(counts))


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == '__main__':
    # this is how we call a Map-Reduce job in `mrjob`:
    MRWordFreqCount.run()
