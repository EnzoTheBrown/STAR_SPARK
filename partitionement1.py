from pyspark import SparkContext
import numpy as np
sc = SparkContext()

# return une hashmap pour l'index des colonnes du csv
def readsql(sc, filename):
	res = sc.textFile(filename)
	# get the first line
	header = res.first()
	data = res.filter(lambda line: line != header).map(lambda line: line.split()[0]).collect()[:-1]
	hashh = {}
	for attribute in data:
		hashh[attribute] = data.index(attribute) 
	return hashh

def getMax(data, sources):
    return data\
        .map(lambda line: (float(line[sources['ra']]), float(line[sources['ra']])
                           , float(line[sources['decl']]), float(line[sources['decl']])))\
        .reduce(lambda a, b: [min(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), max(a[3], b[3])])

def subGrid(square):
    xmin, xmax, ymin, ymax = square 
    return [
            [xmin, (xmin + xmax)/2, ymin, (ymin + ymax)/2, '1'],
            [(xmin + xmax)/2, xmax, ymin, (ymin + ymax)/2, '2'],
            [(xmin + xmax)/2, xmax, (ymin + ymax)/2, ymax, '3'],
            [xmin, (xmin + xmax)/2, (ymin + ymax)/2, ymax, '4']
        ]
def findsquare(grid, ra, decl):
    for v in grid:
        if ra >= v[0] and ra <= v[1] and decl >= v[2] and decl <= v[3]:
            return v[4]
    raise ValueError(grid, ra, decl)

sources = readsql(sc, 'p1206976/schema/Source.sql')
rdd = sc.textFile('/tp-data/Source/Source-001.csv')\
    .map(lambda x: x.split(','))\
    .filter(lambda line: len(line) > 2)
grid = subGrid(getMax(rdd, sources))
rdd = rdd.map(lambda line: [findsquare(grid, float(line[sources['ra']]), float(line[sources['decl']]))] + line)

for name in range(4):
    rdd.filter(lambda line: line[0] == str(name))\
        .map(lambda line: line[1:])\
        .saveAsTextFile('p1206976/'+str(name))

