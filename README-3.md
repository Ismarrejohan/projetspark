# Projet Spark Streaming - graphe LeBonCoin

Projet de big data. En gros on simule un flux d'interactions style LeBonCoin (un
user aime / veut / achete un produit d'un vendeur), Spark agrege tout ca par
fenetres de temps, on en fait un graphe avec GraphFrames, et un dashboard l'affiche
en direct.

simulateur -> spark streaming -> dashboard

## prerequis

python 3 + Java 8 ou 11 (obligatoire pour spark)

pip install pyspark pandas networkx matplotlib

## graphframes

pas inclus dans spark, il se telecharge tout seul au premier lancement (faut
internet la premiere fois). par contre la version doit matcher avec votre version
de spark :

* spark 3.5 -> graphframes:graphframes:0.8.4-spark3.5-s\_2.12
* spark 3.4 -> graphframes:graphframes:0.8.3-spark3.4-s\_2.12
* spark 3.3 -> graphframes:graphframes:0.8.2-spark3.3-s\_2.12

pour voir votre version : lancer `pyspark`, le numero s'affiche au demarrage.

## lancement

faut 3 terminaux, dans le dossier du projet, dans cet ordre :

1. python3 simulateur.py
2. python3 streaming\_spark.py   (laisser \~15s qu'il demarre)
3. python3 dashboard.py

au bout de 15-20s une fenetre s'ouvre avec le graphe, et ca se met a jour toutes
les 3 secondes.

## fichiers

* simulateur.py -> genere le flux json (\~10 events/s, 1 fichier tous les 10 events)
* streaming\_spark.py -> lit le flux, fenetrage + watermark, graphe, export csv
* dashboard.py -> lit les csv et dessine le graphe
* streaming\_data/ -> cree tout seul, contient les fichiers du flux
* live\_edges.csv / live\_nodes.csv -> le pont entre spark et le dashboard

