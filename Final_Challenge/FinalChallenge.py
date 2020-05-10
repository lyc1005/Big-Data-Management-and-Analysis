#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import csv
import numpy as np
import sys

county2idx = {'MAN':1,'MH':1,'MN':1,'NEWY':1,'NEW Y':1,'NY':1,\
              'BRONX':2,'BX':2,'PBX':2,\
              'BK':3,'K':3,'KING':3,'KINGS':3,\
              'Q':4,'QN':4,'QNS':4,'QU':4,'QUEEN':4,\
              'R':5,'RICHMOND':5}
# 1 = Manhattan
# 2 = Bronx
# 3 = Brooklyn
# 4 = Queens
# 5 = Staten Island

def processCenterline(pid, records):
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        try:
            pysicalID = int(row[3])
            street = row[28].lower()
            boro = int(row[13])
            l_low = int(row[0])
            l_high = int(row[1])
            r_low = int(row[4])
            r_high = int(row[5])
        except:
            continue
        yield (pysicalID, street, boro, l_low, l_high, 1)
        yield (pysicalID, street, boro, r_low, r_high, 0)


def processViolation(pid, records):
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        try:
            year = int(row[5][-4:])
            house = int(row[24])
            street = row[25].lower()
            if row[22] in county2idx.keys():
                boro = county2idx[row[22]]
            else:
                raise ValueError
            is_left = house%2
        except:
            continue
        yield (year, house, street, boro, is_left)


def processformat(records):
    for r in records:
        if r[0][1]==2019:
            yield (r[0][0], (r[1], 0))
        else:
            yield (r[0][0], (0, r[1]))


def compute_ols(y, x=list(range(2018,2020))):
    x, y = np.array(x), np.array(y)
    # number of observations/points 
    n = np.size(x
    # mean of x and y vector 
    m_x, m_y = np.mean(x), np.mean(y)
    # calculating cross-deviation and deviation about x 
    SS_xy = np.sum(y*x) - n*m_y*m_x 
    SS_xx = np.sum(x*x) - n*m_x*m_x
    # calculating regression coefficients 
    coef = SS_xy / SS_xx
    return coef


def process(i):
    if ',' in str(i):
        return "\"{}\"".format(i)
    else:
        return str(i)


def to_csv(rdd):
    li = map(process, rdd)
    return ','.join(li)


if __name__ == "__main__":

    output = sys.argv[1]

    sc = SparkContext()
    spark = SparkSession(sc)

    centerline = sc.textFile('nyc_cscl.csv')

    rdd_cl = centerline.mapPartitionsWithIndex(processCenterline)

    violations = sc.textFile('Parking Violations Issued - Fiscal Year 2019.csv')

    rdd_v = violations.mapPartitionsWithIndex(processViolation)

    v = spark.createDataFrame(rdd_v, ('year','house','street','boro','is_left'))

    cl = spark.createDataFrame(rdd_cl, ('pysicalID','street','boro','low','high','is_left'))

    cond = [v.boro == cl.boro,
            v.street == cl.street,
            v.is_left == cl.is_left,
            (v.house >= cl.low) & (v.house <= cl.high)]

    df_2019 = v.join(cl, cond, 'inner').groupBy([cl.pysicalID, v.year]).count()

    df_2019.rdd.map(lambda x: ((x[0], x[1]), x[2]))\
               .mapPartitions(processformat)\
               .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))\
               .mapValues(lambda y: y+(compute_ols(y=list(y)),))\
               .sortByKey()\
               .map(lambda x: ((x[0],)+x[1]))\
               .map(to_csv)\
               .saveAsTextFile(output)

