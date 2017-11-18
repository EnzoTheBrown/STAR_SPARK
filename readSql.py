from pyspark import SparkContext

sc = SparkContext('local', "My Kunfu Is The Best")

def readsql(sc, filename):
	res = sc.textFile(filename)
	# get the first line
	header = res.first()
	data = res.filter(lambda line: line != header)\
		.map(lambda line: filter((lambda x: x!= ''), line.split(' '))[0])\
		.collect()[:-1]
	hashh = {}
	for attribute in data:
		hashh[attribute] = data.index(attribute) 
	return hashh


object_attributes = readsql(sc, 'p1206976/schema/Object.sql')
sources_attributes = readsql(sc, 'p1206976/schema/Source.sql')

def cutRect(nb_partitions, coords):
	ra_max = coords[0]
	ra_min = coords[1]
	decl_max = coords[2]
	decl_min = coords[3]
	ra_step = (ra_max - ra_min)/nb_partitions
	decl_step = (decl_max - decl_min)/nb_partitions
	c=[]
	for i in range(nb_partitions):
		tmp = []
		for j in range(nb_partitions):
			tmp.append([ra_min + i*ra_step, ra_min + (i+1)*ra_step, decl_min + j*decl_step, decl_min +(j+1)*decl_step, str(i)+str(j)])
		c += tmp
	return c


def getMax(sc, filename, objects, sources, nb_part):
	res = sc.textFile(filename)
	data = res\
		.map(lambda line: line.split(',')).cache()
	data2 = data\
		.map(lambda line: (1, [1, float(line[sources['ra']]), float(line[sources['ra']]), float(line[sources['decl']]), float(line[sources['decl']])]))\
		.reduceByKey(lambda a, b: ['', max(a[1], b[1]), min(a[2], b[2]), max(a[3], b[3]), min(a[4], b[4])])\
		.collect()[-1][-1][1:]
	grid = cutRect(nb_part, data2)
	def find_square(grid, ra, decl):
		for square in grid:
			if ra >= square[0] and ra <= square[1] and decl >= square[2] and decl <= square[3]:
				return square[-1]
		print(grid, ra, decl)
		return "-1"
	#print(data.map(lambda l: (float(l[sources['ra']]), float(l[sources['decl']]))).take(5))
	print('#'*30)
	#print(grid)
	rdd2 = data\
		.map(lambda line: (find_square(grid, float(line[sources['ra']]), float(line[sources['decl']])), [line]))\
		.map(lambda line: (line[0], line[1] + [line[0]]))\
		.reduceByKey(lambda a, b: a+b).cache()
	names = [i[-1] for i in grid]	
	def toCSVLine(data):
		return ','.join(str(d) for d in data)
	print(rdd2.filter(lambda line: line[-1] == '00').collect())#.map(toCSVLine).saveAsTextFile('p1206976/'+'00'+'.csv')



getMax(sc, 'p1206976/Source/Source-001.csv', object_attributes, sources_attributes, 8)
# on coupe en 4*4 rectangles


def sparkTPApp5(sc, filename, objects, sources):
	res = sc.textFile(filename)
	def euclidean_distance(x, y):
		return (x[0] - y[0])**2 + (x[1] - y[1])**2
	data = res\
		.map(lambda line: line.split(','))\
		.map(lambda line: (line[sources['objectId']], [1, atoi(line[sources['sourceId']]), line[sources['ra']], line[sources['ra']], line[sources['decl']], line[sources['decl']]]))\
		.take(5)

