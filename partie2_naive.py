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
        .map(lambda line: (float(line[sources['ra'] + 1]), float(line[sources['ra'] + 1])
                           , float(line[sources['decl'] + 1]), float(line[sources['decl'] + 1])))\
        .reduce(lambda a, b: [min(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), max(a[3], b[3])])

def testGetMax(sc):
    sources = {'ra': 5, 'decl': 7}
    data = sc.parallelize(np.random.rand(8, 100))
    # on test si les resultats sont coherents
    assert [0,1,0,1] == list(map(round, getMax(data, sources))), "getMax not OK"
    print('getMax Ok')


# retourne les quatres sous-grille d'une grille
def subGrid(square):
    xmin, xmax, ymin, ymax = square 
    return [
            [xmin, (xmin + xmax)/2, ymin, (ymin + ymax)/2],
            [(xmin + xmax)/2, xmax, ymin, (ymin + ymax)/2],
            [(xmin + xmax)/2, xmax, (ymin + ymax)/2, ymax],
            [xmin, (xmin + xmax)/2, (ymin + ymax)/2, ymax]
        ]

def cutGrid(a, grid):
    res = grid
    for k, v in a.items():
        sg = subGrid(grid[k])
        for i in range(4):
            res[k+str(i)]=sg[i]
        res.pop(k)
    #print("res:", res)
    return res

def findsquare(grid, ra, decl):
    for k, v in grid.items():
        if ra >= v[0] and ra <= v[1] and decl >= v[2] and decl <= v[3]:
            return k
    raise ValueError(grid, ra, decl)

def saveMap(sources, rdd, seuil):
    a = {}
    rdd = rdd.map(lambda line: ['_'] + line.split(',')).cache()
    grid = {'_':getMax(rdd, sources)}
    while True:
        a = rdd.countByKey()
        a = {k:v for k,v in a.items() if v > seuil}
        if not len(a):
            break
        #print("grid", grid)
        grid = cutGrid(a, grid)
        #print("grid", grid)
        rdd = rdd.map(lambda line: [findsquare(grid, float(line[sources['ra'] + 1])
                                           , float(line[sources['decl'] + 1]))]+line[1:])

    keys = rdd.countByKey().items()

    names = [i[0] for i in keys]
    for name in names:
        rdd.filter(lambda line: line[0] == name)\
            .map(lambda line: line[1:])\
            .saveAsTextFile('p1206976/'+name)

    print(keys)

testGetMax(sc)
testGetKey(sc)

sources = readsql(sc, 'p1206976/schema/Source.sql')
rdd = sc.textFile('/tp-data/Source/Source-*.csv')
seuil = 7
saveMap(sources, rdd, seuil)

sc.stop()

