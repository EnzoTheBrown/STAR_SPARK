
# partie 2 façon naïve
## objectif: 
Nous allons appliquer l'algorithme précédent de manière recursive jusqu'à obtenir des rectangles de taille convenable:
![Image of project](https://github.com/EnzoTheBrown/bda_spark/raw/master/algo.png)


```python
from pyspark import SparkContext
# init spark context
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
print("#"*80+"\n\n")
print("#"*8 + " "*27 +"readSQL done" +" "*27 + "#" *8+"\n\n")
print("#"*80)
# retourne : [min_ra, max_ra, min_decl, max_decl]
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
testGetMax(sc)

# retourne : [(key1, value1), (key2, value2), ...] 
def getKey(rdd):
    return rdd.map(lambda l: (l[0], 1))\
        .reduceByKey(lambda a, b: a+b).collect()
    
def testGetKey(sc):
    testrdd = sc.parallelize([["aaa"]]*10 + [["bbb"]]*15 + [["ccc"]]*10)
    assert set([("aaa", 10), ("bbb", 15), ("ccc",10)]) == set(getKey(testrdd)), "getKey not OK"
    print('getKey OK')
testGetKey(sc)

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
    for k, v in a:
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
        a = getKey(rdd)
        a = list(filter(lambda x : x[1] > seuil, a))
        if not len(a):
            break
        #print("grid", grid)
        grid = cutGrid(a, grid)
        #print("grid", grid)
        rdd = rdd.map(lambda line: [findsquare(grid, float(line[sources['ra'] + 1])
                                           , float(line[sources['decl'] + 1]))]+line[1:])

    keys = rdd.map(lambda line: (line[0], 1)).reduceByKey(lambda a, b: a + b).collect()

    names = [i[0] for i in keys]
    print(rdd.take(1))
    for name in names:
        rdd.filter(lambda line: line[0] == name)\
            .map(lambda line: line[1:])\
            .saveAsTextFile('/me/p1206976/'+name+'.csv')

    print(keys)

sources = readsql(sc, '/me/p1206976/schema/Source.sql')
rdd = sc.textFile('/me/p1206976/Source/Source-001.csv')
seuil = 7
saveMap(sources, rdd, seuil)

sc.stop()

```

    ################################################################################
    
    
    ########                           readSQL done                           ########
    
    
    ################################################################################
    getMax Ok
    getKey OK
    [['_21', '29710725217517768', '453349688988', '3', 'NULL', 'NULL', '0', '358.08941191498536', '0.0000833115', '0', '3.039583881799817', '0.0000440547', '0', '13195723847569', 'NULL', 'NULL', 'NULL', 'NULL', '358.09409958603084', '0', '3.0181249038638107', '0', 'NULL', 'NULL', '358.09409958603084', '3.0181249038638107', '11.145301519897007', '1.53915', '396.37843658437066', '0.725266', '358.08941191498536', '0.0000833115', '3.039583881799817', '0.0000440547', '358.08941191498536', '3.039583881799817', '50984.37021041665', '15', '748.4060503461875', '162.566', '1602.0987205576666', '412.589', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', '0', '13.7293', '7.78971', '3.56652', '3.58018', '5.5378', '2.02357', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', '0', '0', '0', 'NULL', 'NULL', 'NULL', '1067.139667359477', '3190.48', '2034.2137881244207', '296.163', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', 'NULL', '0', '129', '0']]
    [('_22', 7), ('_0003', 4), ('_33333330', 4), ('_33333333', 6), ('_21', 3), ('_0000', 5), ('_003', 1)]



```python
sc.stop()
```
