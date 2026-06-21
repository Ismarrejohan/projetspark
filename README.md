# Projet Spark Streaming – Graphe d'interactions "LeBonCoin"

Pipeline temps réel : un simulateur génère un flux JSON infini, PySpark Structured
Streaming l'agrège par fenêtres de temps et modélise un graphe avec GraphFrames,
puis un dashboard affiche le graphe qui se rafraîchit tout seul.

```
Simulateur  ->  PySpark Structured Streaming  ->  Graphe (GraphFrames)  ->  Dashboard
(JSON Lines)     (fenêtrage + watermark)          (degré / centralité)      (networkx)
```

## 1. Prérequis

- Python 3.x et **Java 8 ou 11** (obligatoire pour Spark)
- Bibliothèques Python :
  ```bash
  pip install pyspark pandas networkx matplotlib
  ```

## 2. GraphFrames

GraphFrames n'est pas inclus dans Spark : il est téléchargé automatiquement au
premier lancement grâce à la ligne `spark.jars.packages` (il faut donc Internet
au premier run). **La version doit correspondre à votre version de Spark.**

Pour connaître votre version :
```bash
pyspark   # puis regarder le numéro affiché, ou taper spark.version
```

| Version de Spark | Package à mettre dans `streaming_spark.py`            |
|------------------|-------------------------------------------------------|
| 3.5.x            | `graphframes:graphframes:0.8.4-spark3.5-s_2.12`       |
| 3.4.x            | `graphframes:graphframes:0.8.3-spark3.4-s_2.12`       |
| 3.3.x            | `graphframes:graphframes:0.8.2-spark3.3-s_2.12`       |

> Si GraphFrames refuse de se charger le jour J, ce n'est pas bloquant : le script
> contient un filet de sécurité (`try/except`) qui calcule quand même le degré des
> nœuds, donc le dashboard continue de fonctionner.

## 3. Lancement (3 terminaux)

L'ordre est important. Ouvrir **3 terminaux** dans le dossier du projet.

```bash
# Terminal 1 — le producteur de données
python3 simulateur.py

# Terminal 2 — le traitement Spark (attendre ~15 s qu'il démarre)
python3 streaming_spark.py
# ou, plus explicite :
# spark-submit --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 streaming_spark.py

# Terminal 3 — la visualisation
python3 dashboard.py
```

Au bout de 15–20 secondes, une fenêtre s'ouvre avec le graphe qui se met à jour
toutes les 3 secondes.

## 4. Fichiers

| Fichier               | Rôle                                                              |
|-----------------------|-------------------------------------------------------------------|
| `simulateur.py`       | Génère le flux d'événements JSON (10 events/s, 1 fichier / 10 events) |
| `streaming_spark.py`  | Lecture du flux, fenêtrage, watermark, graphe GraphFrames, export CSV |
| `dashboard.py`        | Lit les CSV et dessine le graphe (networkx + matplotlib)          |
| `streaming_data/`     | Dossier créé automatiquement, contient les fichiers du flux       |
| `live_edges.csv` / `live_nodes.csv` | Pont entre Spark et le dashboard (réécrits à chaque batch) |
