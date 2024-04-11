import psycopg2
import matplotlib.pyplot as plt

# Connexion à la base de données
try:
    conn = psycopg2.connect(
        dbname="nyc_datamart",
        user="postgres",
        password="admin",
        host="localhost",
        port="5432"
    )
except Exception as e:
    print(f"Erreur lors de la connexion à la base de données : {e}")

# Création d'un curseur
cur = conn.cursor()

# Exécution de la requête SQL pour obtenir le nombre de courses par fournisseur
query = """
    SELECT vendorid, COUNT(*) as count
    FROM public.FactTable
    GROUP BY vendorid
"""

cur.execute(query)

# Récupération des résultats
results = cur.fetchall()

# Fermeture du curseur et de la connexion
cur.close()


# Préparation des données pour la visualisation
vendors = [result[0] for result in results]
counts = [result[1] for result in results]

# Création de sous-graphiques pour chaque fournisseur
fig, ax = plt.subplots()

for vendor, count in zip(vendors, counts):
    ax.bar(int(vendor), count, label=f'{"Creative Mobile Technologies,vendor" if vendor == 1 else "VeriFone Inc."}')


ax.set_ylabel('Nombre de courses')
ax.set_title('Nombre de courses par fournisseur')
ax.legend()
ax.set_xticks([])

plt.show()

cur = conn.cursor()

# Exécution de la requête SQL pour obtenir le nombre de courses par méthode de paiement
query = """
    SELECT payment_type, COUNT(*) as count
    FROM public.FactTable
    GROUP BY payment_type
"""

cur.execute(query)

# Récupération des résultats
results = cur.fetchall()

# Fermeture du curseur et de la connexion
cur.close()
# Préparation des données pour la visualisation
payment_types = [result[0] for result in results]
counts = [result[1] for result in results]

# Création de sous-graphiques pour chaque méthode de paiement
fig, ax = plt.subplots()

for payment_type, count in zip(payment_types, counts):
    match payment_type : 
        case 1 : 
            payment_type = 'Credit card'
        case 2 : 
            payment_type =  'Cash'
        case 3 : 
            payment_type = 'No charge'
        case 5 : 
            payment_type = 'Unknown'
        case 4 : 
            payment_type = 'Dispute'
        case 6 : 
            payment_type = 'Voided trip' 
        case _ : 
            payment_type = ' '       
    ax.bar(str(payment_type), count, label=f'Méthode de Paiement {payment_type}')

ax.set_title('Méthode de paiement')
ax.legend()

# Masquer les axes des x et y
ax.get_xaxis().set_visible(False)
ax.get_yaxis().set_visible(False)

plt.show()

# Création d'un curseur
cur = conn.cursor()

# Exécution de la requête SQL pour obtenir la distance moyenne par fournisseur (vendor)
query_distance = """
    SELECT
        vendorid,
        AVG(trip_distance) AS moyenne_distance
    FROM
        public.FactTable
    WHERE
        vendorid IN (1, 2)  -- Filtre par les fournisseurs 1 et 2
    GROUP BY
        vendorid;
"""

cur.execute(query_distance)

# Récupération des résultats
results_distance = cur.fetchall()

# Exécution de la requête SQL pour obtenir le nombre de courses par fournisseur (vendor)
query_course_count = """
    SELECT
        vendorid,
        COUNT(*) AS course_count
    FROM
        public.FactTable
    GROUP BY
        vendorid;
"""

cur.execute(query_course_count)

# Récupération des résultats
results_course_count = cur.fetchall()

# Fermeture du curseur et de la connexion
cur.close()
conn.close()

# Préparation des données pour la visualisation de la distance moyenne
vendors_distance = [result[0] for result in results_distance]
average_distances = [result[1] for result in results_distance]

# Préparation des données pour la visualisation du nombre de courses
vendors_course_count = [result[0] for result in results_course_count]
counts = [result[1] for result in results_course_count]

# Création d'un graphique à barres pour la distance moyenne par fournisseur (vendor) avec des couleurs différentes
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(8, 10))

# Ajout de la légende et des libellés des axes pour la distance moyenne
colors_distance = ['blue' if vendor == 1 else 'orange' for vendor in vendors_distance]
labels_distance = ['Creative Mobile Technologies,vendor' if vendor == 1 else 'VeriFone Inc.' for vendor in vendors_distance]
ax1.bar(labels_distance, average_distances, color=colors_distance)
ax1.set_ylabel('Distance Moyenne')
ax1.set_title('Distance Moyenne par Fournisseur (Vendor)')

# Ajout de la légende et des libellés des axes pour le nombre de courses
colors_course_count = ['blue' if vendor == 1 else 'orange' for vendor in vendors_course_count]
labels_course_count = ['Creative Mobile Technologies,vendor' if vendor == 1 else 'VeriFone Inc.' for vendor in vendors_course_count]
bars = ax2.bar(labels_course_count, counts, color=colors_course_count)
ax2.set_ylabel('Nombre de Courses')
ax2.set_title('Nombre de Courses par Fournisseur (Vendor)')

# Désactivation de l'axe des x dans les deux sous-graphiques
ax1.get_xaxis().set_visible(False)
ax2.get_xaxis().set_visible(False)

# Ajout des labels pour chaque barre dans le deuxième graphique
for bar, label in zip(bars, counts):
    height = bar.get_height()
    ax2.text(bar.get_x() + bar.get_width() / 2, height, label, ha='center', va='bottom')

plt.tight_layout()
plt.show()