import json
import time
import random
from datetime import datetime, timezone
import os

# Dossier surveille par Spark (source du flux)
os.makedirs("streaming_data", exist_ok=True)

actions = ["AIME", "VEUT", "ACHAT"]
villes = ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux"]
categories = ["Vehicules", "Immobilier", "Informatique", "Mobilier"]

vendeurs = [f"sel_{10 + i}" for i in range(5)]

# Catalogue FIXE : un produit garde toujours le meme vendeur
# (sinon le lien Vendeur -> Produit du graphe n'a pas de sens)
catalogue = []
for i in range(20):
    catalogue.append({
        "product_id": f"prod_{100 + i}",
        "product_cat": random.choice(categories),
        "seller_id": random.choice(vendeurs),
        "price": round(random.uniform(10.0, 5000.0), 2),
    })

utilisateurs = [
    {"user_id": f"usr_{1000 + i}", "user_city": random.choice(villes)}
    for i in range(15)
]


def generer_evenement():
    user = random.choice(utilisateurs)
    produit = random.choice(catalogue)
    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "user_id": user["user_id"],
        "user_city": user["user_city"],
        "product_id": produit["product_id"],
        "product_cat": produit["product_cat"],
        "seller_id": produit["seller_id"],
        "action_type": random.choice(actions),
        "price": produit["price"],
    }


print("Demarrage du simulateur par lots de 10... (Ctrl+C pour arreter)")
compteur = 1
batch_actuel = []

while True:
    batch_actuel.append(generer_evenement())

    # 1 fichier tous les 10 evenements, au format JSON Lines (1 objet/ligne)
    if compteur % 10 == 0:
        filename = f"streaming_data/batch_{compteur}.json"
        with open(filename, "w") as f:
            for event in batch_actuel:
                f.write(json.dumps(event) + "\n")
        print(f"Lot {compteur} genere.")
        batch_actuel = []

    compteur += 1
    time.sleep(0.1)  # ~10 evenements / seconde
