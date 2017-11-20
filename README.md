# partie 1 
## Objectif:
Considérons que les données de base sont mélangées dans plusieurs fichiers, nous allons devoir regrouper les donnée en fonction de leur proximité géographique pour par exemple facilité des futurs calculs ou autre.

## Réalisation 1 :
### le code est disponible dans les fichiers partitionement1.py et partie1.py

Le premièr algorithme est simple et peu couteux, nous allons récuperer les coordonnée extrémales de ra et decl pour ensuite en deduire une surface englobant toutes les données.
``` python
def getMax(data, sources):
    return data\
        .map(lambda line: (float(line[sources['ra'] + 1]), float(line[sources['ra'] + 1])
                           , float(line[sources['decl'] + 1]), float(line[sources['decl'] + 1])))\
        .reduce(lambda a, b: [min(a[0], b[0]), max(a[1], b[1]), min(a[2], b[2]), max(a[3], b[3])])
```

Maintenant que nous avons notre surface englobante, nous pouvons la découper pour diviser notre jeux de donnée, et garder les données géographiquement liées enssemble:
```python
def cutRect(nb_partitions, coords):
	ra_max = coords[0]
	ra_min = coords[1]
	decl_max = coords[2]
	decl_min = coords[3]
	ra_step = (ra_max - ra_min)/nb_partitions
	decl_step = (decl_max - decl_min)/nb_partitions
	grid=[]
	for i in range(nb_partitions):
		tmp = []
		for j in range(nb_partitions):
			tmp.append([ra_min + i*ra_step, ra_min + (i+1)*ra_step, 
                        decl_min + j*decl_step, decl_min +(j+1)*decl_step, str(i)+str(j)])
		grid += tmp
	return grid
```

Nous obtenons ainsi un grille contenant nb_partition² rectangles et chaque rectangle a un identifiant unique.

Maintenant nous pouvons affecter un rectangle à chaque ligne de notre RDD.

```python
def findsquare(grid, ra, decl):
    for k, v in grid.items():
        if ra >= v[0] and ra <= v[1] and decl >= v[2] and decl <= v[3]:
            return k
    raise ValueError(grid, ra, decl)

rdd.map(
    lambda line: [findsquare(
    grid,
    float(line[sources['ra'] + 1]),
    float(line[sources['decl'] + 1]))
                 ]+line[1:]
)
```

Une fois les données affectées à différentes zones géographique, nous pouvons les sauvegarder dans les fichiers correspondant:

```python
# on récupère le nom de chaque zone
names = [i[-1] for i in grid]
print(rdd3.collect())
for name in names:
    # pour chaque nom, on sauve les données correspondante dans un fichier csv
	rdd2.filter(lambda line: line[0] == name)\
		.map(lambda line: line[1])\
		.saveAsTextFile('p1206976/'+name)
```

### Conclusion :
C'est algorithme simple et peu couteux, mais les zones ne sont pas du tout homogènes.

## Réalisation 2 :
### le code est disponible dans les fichiers kmeans.py
Le second algorithme est celui des Kmeans, 

```python
from pyspark import SparkContext
sc = SparkContext()

from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

# Load and parse the data
sources = readsql(sc, 'p1206976/schema/Source.sql')
data = sc.textFile('/tp-data/Source/Source-001.csv')
parsedData = data.map(lambda line: array([float(x) for x in (line[sources['ra']], line[sources['decl']])]))

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 5, maxIterations=10,
        runs=10, initializationMode="random")


clusters_int = clusters.predict(parsedData)
data = clusters_int.zip(data).countByKey()
print(data)
sc.stop()
```
![kmeans result](https://github.com/EnzoTheBrown/bda_spark/blob/master/kmeans.png?raw=true)

### Conclusion Kmeans:
L'algorithme donnes des resultats relativement bon car il tend a faire des clusters de même taille.


# partie 2 façon naïve
### le code est disponible dans le fichier partie2_naive.py
## objectif: 
Nous allons appliquer l'algorithme précédent de manière recursive jusqu'à obtenir des rectangles de taille convenable:
![Image of project](https://github.com/EnzoTheBrown/bda_spark/raw/master/algo1.png)


### retourner les extrema d'un rdd
la fonction getMax :: RDD -> {String:Int} -> (Int, Int, Int, Int) 
permet de recuperer les mins et les max de ra et decl en fonction d'un rdd et du dictionnaire généré par readsql qui va nous permettre de renvoyer l'index de ra et decl
#### test
pour tester la fonction, nous avons generé aléatoirement des lignes et on regarde si les resultats obtenus sont cohérents


## opérations sur la grille
Notre grille est composée de plusieurs carrés, que nous pouvons découpés à loisir pour homogénéïsé la quantité d'information par fichier

### description des fonctions
subSquare :: rect -> (rect, rect, rect, rect)

cutGrid :: [(key, value)] -> {key, rect} -> {key, rect}
regarde chaque rectangle de la grille, appel la fonction subGrid pour le découper.

findsquare :: {key, rect} -> float -> float -> key
determine de quelle rectangle un point (ra, decl) fait partie et renvoit la clé de ce rectangle
cela nous permet alors d'affecter alors un identifiant à chaque ligne de notre rdd pour ainsi collecter les données géographiquement proche dans le même fichier.

## itérations sur le rdd
tant qu'il existe des rectangles de trop grande taille, on continue à les découper et on affecte les lignes du rdd à leur rectangle respectif.
puis on sauvegarde le tous dans des fichier csv au nom du rectangle.

## application sur 5Go de donnée
Sur le cluster nous avons reussis à faire tourner notre algorithme en moins de 10 minutes et nous avons obtenu les résultats suivant:
![taille des fichiers](https://github.com/EnzoTheBrown/bda_spark/blob/master/files_spark.png?raw=true)

on peut remarquer en regardant le nom des fichiers sur l'axe des y qu'il a fallut autour de 7 itérations de notre algorithme pour regrouper nos données.

# partie 2 façon merge
Pour homogénéiser la taille des fichiers, nous avons ajouté la fonctionnalité de merge pour fusionner les fichiers de petite taille avec leurs voisins.

## résultats obtenus:
![taille des fichiers](https://github.com/EnzoTheBrown/bda_spark/blob/master/merge.png?raw=true)
