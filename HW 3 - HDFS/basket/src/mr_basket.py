#! /usr/bin/env python

from itertools import permutations
from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class MRBasket(MRJob):
    """
    A class to count item co-occurrence in shopping baskets
    """

    def mapper_get_session_items(self, _, line):
        """

        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) pairs
        """

        # TODO: start implementing here!
        # ...
        # yield ...
        reader = csv.reader([line])
        for word in reader:
            id, date, item = word[0], word[1], word[2]
            yield ((id, date), item)


    def reducer_all_combinations(self, user_date, products):
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
        #permutations function filters out if only one unique item is bought by the user on a particular date.
        all_combinations = list(permutations(list(set(products)),2)) #set(products) will filter out for every key the products that repeated. so say if a person bought 2 milk cans on the same date, then this will filter out the redundant entry of the seconds milk can.
        for i in all_combinations:
            yield (i,1)

    def reducer_combination_counts(self, product_pairs, count):
        yield(product_pairs[0], (product_pairs[1], sum(count)))
    
    def reducer_best_combination(self, products, freq):
        sorted_freq=sorted(freq,key=lambda x:x[1])
        yield(products,sorted_freq[-1])

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_get_session_items,
                reducer=self.reducer_all_combinations),
            MRStep(reducer=self.reducer_combination_counts),
            MRStep(reducer = self.reducer_best_combination)
        ]


# this '__name__' == '__main__' clause is required: without it, `mrjob` will
# fail. The reason for this is because `mrjob` imports this exact same file
# several times to run the map-reduce job, and if we didn't have this
# if-clause, we'd be recursively requesting new map-reduce jobs.
if __name__ == "__main__":
    # this is how we call a Map-Reduce job in `mrjob`:
    MRBasket.run()
