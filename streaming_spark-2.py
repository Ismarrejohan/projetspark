from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import window

# SparkSession = point d'entree. shuffle.partitions=2 (petit volume).
# spark.jars.packages charge GraphFrames (version a accorder avec Spark, cf README).
spark = SparkSession.builder \
    .appName("LeBonCoin_GraphStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema strict : on impose les colonnes/types (plus rapide, pas d'erreur de type)
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True),
])

# Lecture du flux : Spark surveille le dossier et lit chaque nouveau fichier
flux_df = spark.readStream.schema(schema).json("streaming_data/")

# Comptage par fenetres de temps
# - watermark 10 s : tolere les retards + libere la memoire (AVANT le groupBy)
# - window(10s, 5s) : fenetre glissante (largeur 10 s, pas de 5 s)
agregation_df = flux_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window("timestamp", "10 seconds", "5 seconds"),
        "action_type",
        "user_id",
        "product_id",
        "seller_id"
    ).count()


def traiter_batch(batch_df, batch_id):
    # foreachBatch : DataFrame "normal" -> on peut construire le graphe + exporter
    if batch_df.rdd.isEmpty():
        return

    donnees = batch_df.drop("window")

    # Aretes : Utilisateur -> Produit (action + poids = count)
    liens_users = donnees.select("user_id", "product_id", "action_type", "count")
    liens_users = liens_users.withColumnRenamed("user_id", "src")
    liens_users = liens_users.withColumnRenamed("product_id", "dst")
    liens_users = liens_users.withColumnRenamed("action_type", "relationship")
    liens_users = liens_users.withColumnRenamed("count", "weight")

    # Aretes : Vendeur -> Produit (PROPOSE)
    liens_vendeurs = donnees.select("seller_id", "product_id").distinct()
    liens_vendeurs = liens_vendeurs.withColumnRenamed("seller_id", "src")
    liens_vendeurs = liens_vendeurs.withColumnRenamed("product_id", "dst")
    liens_vendeurs = liens_vendeurs.withColumn("relationship", F.lit("PROPOSE"))
    liens_vendeurs = liens_vendeurs.withColumn("weight", F.lit(1))

    aretes = liens_users.unionByName(liens_vendeurs)

    # Sommets typés : Utilisateur / Produit / Vendeur
    users = donnees.select("user_id").distinct().withColumnRenamed("user_id", "id")
    users = users.withColumn("type", F.lit("Utilisateur"))
    produits = donnees.select("product_id").distinct().withColumnRenamed("product_id", "id")
    produits = produits.withColumn("type", F.lit("Produit"))
    vendeurs = donnees.select("seller_id").distinct().withColumnRenamed("seller_id", "id")
    vendeurs = vendeurs.withColumn("type", F.lit("Vendeur"))
    noeuds = users.unionByName(produits).unionByName(vendeurs).distinct()

    # Graphe + degre (= indicateur de centralite)
    from graphframes import GraphFrame
    g = GraphFrame(noeuds, aretes)
    degres = g.degrees
    noeuds_avec_degre = noeuds.join(degres, on="id", how="left").fillna(0)

    # Export CSV : le pont vers le dashboard
    aretes.toPandas().to_csv("live_edges.csv", index=False)
    noeuds_avec_degre.toPandas().to_csv("live_nodes.csv", index=False)
    print("Batch", batch_id, "traite et exporte.")


# outputMode("update") = on ne renvoie que ce qui a change a chaque micro-batch
query = agregation_df.writeStream \
    .outputMode("update") \
    .foreachBatch(traiter_batch) \
    .start()

query.awaitTermination()
