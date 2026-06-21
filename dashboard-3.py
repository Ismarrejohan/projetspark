import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import os

# plt.ion() = mode interactif : la fenetre se met a jour toute seule
plt.ion()
fig, ax = plt.subplots(figsize=(13, 9))

# Une couleur par type de noeud
COULEURS = {
    "Utilisateur": "#89CFF0",
    "Produit":     "#FFB347",
    "Vendeur":     "#9BE19B",
}

print("En attente des donnees de Spark (15-20 secondes au debut)...")

# Boucle infinie : le streaming ne s'arrete jamais
while True:

    if os.path.exists("live_edges.csv") and os.path.exists("live_nodes.csv"):
        try:
            # On relit les CSV exportes par Spark
            aretes = pd.read_csv("live_edges.csv")
            noeuds = pd.read_csv("live_nodes.csv")

            if not aretes.empty:

                type_par_id = {}
                degre_par_id = {}
                for i in range(len(noeuds)):
                    point = noeuds.loc[i, "id"]
                    type_par_id[point] = noeuds.loc[i, "type"]
                    if "degree" in noeuds.columns:
                        degre_par_id[point] = noeuds.loc[i, "degree"]

                # Graphe oriente (avec fleches)
                G = nx.DiGraph()
                for i in range(len(aretes)):
                    src = aretes.loc[i, "src"]
                    dst = aretes.loc[i, "dst"]
                    action = aretes.loc[i, "relationship"]
                    poids = aretes.loc[i, "weight"]

                    # Filtre anti-fouillis : liens repetes (>=2), ACHAT et PROPOSE
                    if poids >= 2 or action == "ACHAT" or action == "PROPOSE":
                        G.add_node(src, type=type_par_id.get(src, "Utilisateur"))
                        G.add_node(dst, type=type_par_id.get(dst, "Produit"))
                        G.add_edge(src, dst, label=action)

                if G.number_of_nodes() > 0:

                    couleurs = []
                    tailles = []
                    for point in G.nodes():
                        couleurs.append(COULEURS.get(G.nodes[point]["type"], "#CCCCCC"))
                        degre = degre_par_id.get(point, 1)
                        tailles.append(400 + 250 * degre)  # taille = degre (centralite)

                    ax.clear()
                    ax.set_title("LeBonCoin - Graphe d'interactions temps reel",
                                 fontsize=16, fontweight="bold")

                    pos = nx.spring_layout(G, k=0.8, iterations=50)
                    nx.draw(G, pos, ax=ax, node_color=couleurs, node_size=tailles,
                            with_labels=True, font_size=8, font_weight="bold",
                            edge_color="gray", arrows=True)

                    labels_liens = nx.get_edge_attributes(G, "label")
                    nx.draw_networkx_edge_labels(G, pos, edge_labels=labels_liens,
                                                 ax=ax, font_size=7)

                    legende = [mpatches.Patch(color=COULEURS[t], label=t) for t in COULEURS]
                    ax.legend(handles=legende, loc="upper right")

                    plt.draw()

        except (pd.errors.EmptyDataError, FileNotFoundError, PermissionError):
            # Fichier en cours d'ecriture par Spark : on ignore et on reessaie
            pass

    plt.pause(3)  # rafraichissement toutes les 3 s
