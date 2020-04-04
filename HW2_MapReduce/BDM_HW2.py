#!/usr/bin/env python
# coding: utf-8

from mrjob.job import MRJob


class MRSale(MRJob):

    def mapper(self, _, line):
        row = line.split(',')
        customer = row[0]
        revenue = row[4]        
        yield row[3], (customer, float(revenue)) 

    def reducer(self, key, values):
        customer, revenue = list(zip(*values))
        customer_count = len(set(customer))
        total_revenue = sum(revenue)
        yield key, (customer_count, total_revenue)


if __name__ == '__main__':
    MRSale.run()



