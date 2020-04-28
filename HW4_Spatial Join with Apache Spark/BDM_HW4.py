#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
import csv
import geopandas as gpd
import fiona
import fiona.crs
import shapely
import sys
from heapq import nlargest
import pyproj
import shapely.geometry as geom

def createIndex(geojson):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(geojson).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)


def findZone(p, index, zones):
    '''
    findZone returned the ID of the shape (stored in 'zones' with
    'index') that contains the given point 'p'. If there's no match,
    None will be returned.
    '''
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None


def processTrips(pid, records):
    '''
        Our aggregation function that iterates through records in each
        partition, checking whether we could find a zone that contain
        the pickup location.
        '''
    
    # Create an R-tree index
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    boroughs_index, boroughs_zones = createIndex('boroughs.geojson')
    neighborhoods_index, neighborhoods_zones = createIndex('neighborhoods.geojson')
    
    # Skip the header
    if pid==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        if len(row) == 18:
            try:
                pickup = geom.Point(proj(float(row[5]), float(row[6])))
                dropoff = geom.Point(proj(float(row[9]), float(row[10])))
                # Look up a matching zone, and update the count accordly if
                # such a match is found
                pickup_borough = findZone(pickup, boroughs_index, boroughs_zones)
                dropoff_neighborhood = findZone(dropoff, neighborhoods_index, neighborhoods_zones)
            except:
                continue
            if (pickup_borough != None) and (dropoff_neighborhood != None):
                yield ((boroughs_zones['boroname'][pickup_borough], neighborhoods_zones['neighborhood'][dropoff_neighborhood]), 1)

def process(i):
    if ',' in str(i):
        return "\"{}\"".format(i)
    else:
        return str(i)

def to_csv(rdd):
    li = map(process, rdd)
    return ','.join(li)



if __name__ == "__main__":
    
    sc = SparkContext()
    
    input_file = sys.argv[1]
    output = sys.argv[2]
    
    yellow = sc.textFile(input_file)
    yellow.mapPartitionsWithIndex(processTrips)\
          .reduceByKey(lambda x, y: x+y)\
          .map(lambda x: (x[0][0], x[0][1], x[1]))\
          .groupBy(lambda x: x[0])\
          .flatMap(lambda g: nlargest(3, g[1], key=lambda x: x[2]))\
          .map(lambda x: (x[0], (x[1], x[2])))\
          .reduceByKey(lambda x, y: x+y)\
          .sortByKey()\
          .map(lambda x: ((x[0],)+x[1]))\
          .map(to_csv)\
          .saveAsTextFile(output)
