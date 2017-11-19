from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()

# return une hashmap pour l'index des colonnes du csv
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

def getMax(data, sources):
    return data\
        .map(lambda line: (
            float(line[sources['ra']]),
            float(line[sources['ra']]),
            float(line[sources['decl']]),
            float(line[sources['decl']]))
            )\
        .reduce(lambda a, b: [
            min(a[0], b[0]), 
            max(a[1], b[1]), 
            min(a[2], b[2]), 
            max(a[3], b[3])
            ])

def getKey(rdd):
    return rdd.map(lambda l: (l[0], 1))\
            .reduceByKey(lambda a, b: a+b)\
            .collect()

def subGrid(square):
    xmin, xmax, ymin, ymax = square 
    return [
            [xmin, (xmin + xmax)/2, ymin, (ymin + ymax)/2],
            [(xmin + xmax)/2, xmax, ymin, (ymin + ymax)/2],
            [(xmin + xmax)/2, xmax, (ymin + ymax)/2, ymax],
            [xmin, (xmin + xmax)/2, (ymin + ymax)/2, ymax]
        ]

def cut(a, grid):
    res = grid
    for k, v in a.items():
        sg = subGrid(grid[k])
        for i in range(4):
            res[k+str(i)]=sg[i]
        res.pop(k)
    return res

def findsquare(grid, ra, decl):
    for k, v in grid.items():
        if ra >= v[0] and ra <= v[1] and decl >= v[2] and decl <= v[3]:
            return k
    raise ValueError(grid, ra, decl)

def partition(a_list):
    yield [[x] for x in a_list]   
    for i in range(1, len(a_list) + 1):
        _l = a_list[:]
        yield [_l.pop(i-1), _l]
    yield a_list

def merge_key(key1, key2):
    return key1[0]+key2[0],key1[1]+key2[1]

def dans_seuil(x,s_min=6000):
    return x>= s_min

def merge(l_key,s_min):
    i = 0
    res = l_key[:]
    while i < len(res):
        if dans_seuil(res[i][1],s_min):
            i += 1
        else:
                if i == 0:
                    new_key = merge_key(res[i],res[i+1])
                    res[i] = new_key
                    res.pop(i+1)
                elif i == len(res)-1:
                    new_key = merge_key(res[i-1],res[i])
                    res[i] = new_key
                    res.pop(i-1)
                else:
                    if res[i-1][1]< res[i+1][1]:
                        new_key = merge_key(res[i-1],res[i])
                        res[i] = new_key
                        res.pop(i-1)
                        i -= 1
                    else:
                        new_key = merge_key(res[i],res[i+1])
                        res[i] = new_key
                        res.pop(i+1)
    return res

def getRange(id_part,range_part):
    begin = id_part.find('_')
    res = list(range_part)
    for decoupe in id_part[begin+1:]:
        xmin,xmax,ymin,ymax = res
        if decoupe == '0':
            res = [xmin, (xmin + xmax)/2, ymin, (ymin + ymax)/2]
        elif decoupe == '1':
            res = [(xmin + xmax)/2, xmax, ymin, (ymin + ymax)/2]
        elif decoupe == '2':
            res = [(xmin + xmax)/2, xmax, (ymin + ymax)/2, ymax]
        elif decoupe == '3':
            res = [xmin, (xmin + xmax)/2, (ymin + ymax)/2, ymax]
        elif decoupe == '_':
            yield res[:2],res[2:]
            res = list(id_part)
        else:
            raise ValueError('erreur dans la dcoupe', decoupe)
    yield res[:2],res[2:]

def merged_key(key,l_key):
    for k,_ in l_key:
        if key in k:
            return k
    raise ValueError('pb dans merged_key',key,l_key)

def loop2cut(rdd, seuil, sources, grid):
    a = {}
    i = 0
    while i < 10:
        a = {k:v for k,v in rdd.countByKey().items() if v > seuil}
        rdd = rdd.filter(lambda x: x[0] in a)
        if len(a) > 50:
            break
        print("-"*50,a,"-"*50)
        if not len(a):
            break
        grid = cut(a, grid)
        rdd = rdd.map(lambda line: [findsquare(grid, float(line[sources['ra'] + 1]), float(line[sources['decl'] + 1]))]+line[1:])
        i += 1
    return rdd

sources = readsql(sc, 'p1206976/schema/Source.sql')
rdd = sc.textFile('/tp-data/Source/Source-*.csv')
data = rdd.map(lambda line: line.split(','))


print('m'+'a'*50+'x')
max_range = getMax(data, sources)
grid = {'_':max_range}
rdd = data.map(lambda line: (['_']+ line))
seuil = int(np.mean(list(map(lambda x: 128000000/sys.getsizeof(x), rdd.take(100)))))

loop2cut(rdd, seuil, sources, grid)

cle = [(k,v) for k,v in rdd.countByKey().items()]
cle.sort(key = lambda x : x[0])
print("*"*60,cle)
cle = merge(cle,100000)

rddd = rdd.map(lambda line: (merged_key(line[0],cle),','.join(line[1:])))
print('--'*50,rdd.countByKey())
sqlContext = SQLContext(sc)
rddd = sqlContext.createDataFrame(rdd, ["id_part", "data"])
#rdd.write.partitionBy("id_part").text("test4")

#names = rdd.rdd.keys().collect()
#print(type(names))
#res = ''
#for name in names:
#    res += 'La partition '+ name + 'corespond a\n'
#    res += '\n'.join(['ra dans ' + str(x[0]) + ' et decl dans '+str(x[1]) for x in getRange(name,max_range)])

#print(res)
