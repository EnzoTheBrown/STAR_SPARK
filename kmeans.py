from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel
# Load and parse the data

def readsql(sc, filename):
    res = sc.textFile(filename)
    # get the first line
    header = res.first()
    data = res.filter(lambda line: line != header)\
        .map(lambda line: line.split()[0])\
        .collect()[:-1]
    hashh = {}
    for attribute in data:
        hashh[attribute] = data.index(attribute) 
    return hashh

sc = SparkContext()
sources = readsql(sc, '/me/p1206976/schema/Source.sql')
data = sc.textFile('/me/p1206976/Source/Source-001.csv')
parsedData = data.map(lambda line: line.split(',')).filter(lambda x: len(x)>2).map(lambda line: (float(line[sources['ra']]), float(line[sources['decl']])))

clusters = KMeans.train(parsedData, 10, maxIterations=10, initializationMode="random")

clusters_int = clusters.predict(parsedData).map(lambda x: (x, 1))
a = sorted(clusters_int.countByKey().items(), key=lambda x: x[1])
sc.stop()



