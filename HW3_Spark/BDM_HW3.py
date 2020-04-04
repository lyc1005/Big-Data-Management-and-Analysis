#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
import csv
import sys

if __name__=='__main__':
    
    input_file = sys.argv[1]
    output_folder = sys.argv[2]
 
    sc = SparkContext()

    complaints = sc.textFile(input_file)

    def mapper(rowId, records):
        for rowId in range(3):
            next(records)
        reader = csv.reader(records)
        for r in reader:
            yield ((r[1].lower(), int(r[0][:4]), r[7].lower()), 1)
    
    def to_csv(rdd):
        if ',' in rdd[0]:
            name = "\"{}\"".format(rdd[0])
        else:
            name = rdd[0]
        year = str(rdd[1])
        totol_cpl = str(rdd[2])
        total_cpn = str(rdd[3])
        percentage = str(rdd[4])
        li = [name, year, totol_cpl, total_cpn, percentage]
        return ','.join(li)
    
    
    complaints.mapPartitionsWithIndex(mapper) \
    .reduceByKey(lambda x, y: x+y) \
    .map(lambda x: ((x[0][0], x[0][1]), (x[1], 1, x[1]))) \
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1], max(x[2], y[2]))) \
    .mapValues(lambda x: (x[0], x[1], round(x[2]*100/x[0]))) \
    .sortByKey() \
    .map(lambda x: x[0]+x[1]) \
    .map(to_csv) \
    .saveAsTextFile(output_folder)






