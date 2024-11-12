# GMQ710 TD #2
# Par: Justin Auger et Sarah Breton

# Importation des librairies
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString

# CRÉATION DES VARIABLES GLOBALES DE VALEURS CONSTANTES
crs = 4326
projection = 32618
proximite_arret = 500
test_heure_debut = '11:00:00'
test_heure_fin = '13:00:00'
topX = 5
marge = '06:00:00'
adresse = '1090 RUE DE KINGSTON'
date = '20220211'
heure = '12:00:00'
days_of_week = ['Lundi', 'Mardi', 'Mercredi', 'Jeudi', 'Vendredi']
day_of_week = ['Mercredi']


# CRÉATION DES FONCTIONS
# Fonction qui établit pour un geodataframe un crs et une projection
'''
Fonction: Établir un crs et une projection pour un geodataframe
Entrée: Un geodataframe et les valeurs de l'EPSG pour le crs et la projection
Sortie: Un geodataframe avec le crs et la projection établis
'''
def set_crs_proj(dataframe, epsg_crs, epsg_projection):
    if dataframe.crs is not None and dataframe.crs != f"EPSG:{epsg_crs}":
        dataframe = dataframe.to_crs(epsg=epsg_crs)
    else:
        dataframe.set_crs(epsg=epsg_crs, inplace=True)
    dataframe = dataframe.to_crs(epsg=epsg_projection)
    return dataframe

'''
Fonction: Créer une marge de temps autour d'une heure et un écart de marge donnés
Entrée: Une heure et une marge
Sortie: Les heures minimales et maximales
'''
def creation_marge_temps(test_heure, test_marge):
    heure = pd.to_timedelta(test_heure)
    marge = pd.to_timedelta(test_marge)
    heure_min = heure - marge
    heure_max = heure + marge
    return heure_min, heure_max


'''
Fonction: Valider si une adresse est dans un dataframe
Entrée: Un dataframe et une adresse à tester
Sortie: Un booléen pour indiquer si l'adresse est dans le dataframe
'''
def validation_adresse(dataframe, test_adresse):
    # Vérifier si l'adresse est en majuscule seulement et ajuster sinon
    test_adresse = test_adresse.upper()
    # Vérifier si l'adresse est dans le dataframe
    if test_adresse in dataframe['ADRESSE'].values:
        print('Adresse trouvée')
        return True
    else:
        print('Adresse non trouvée')
        return False
    
'''
Fonction: Créer un buffer autour d'une adresse donnée à une proximité donnée dans un geoDataFrame
Entrée: Un geoDataFrame, une adresse et une valeur de proximité
Sortie: Un geoDataFrame avec un buffer autour de l'adresse
'''
def creation_buffer_autour_adresse(dataframe_address, test_adresse, test_proximite):
    # Trouver le point de l'adresse
    gdf_filtered = dataframe_address[dataframe_address['ADRESSE'] == test_adresse].geometry
    # Buffer autour du point de l'adresse selon la proximité
    gdf_filtered_buffer = gdf_filtered.buffer(test_proximite)
    return gdf_filtered_buffer


'''
Fonction: Créer un buffer autour des arrêts selon la proximité
Entrée: Un geoDataFrame des arrêts et une valeur de proximité
Sortie: Un geoDataFrame avec un buffer autour des arrêts
'''
def creation_buffer_autour_arrets(dataframe_stops, test_proximite):
    # Buffer autour des arrêts selon la proximité
    dataframe_stops_copy = dataframe_stops.copy() 
    dataframe_stops_copy['geometry_buffered'] = dataframe_stops_copy['geometry'].buffer(test_proximite)
    dataframe_stops_buffer = gpd.GeoDataFrame(dataframe_stops_copy.drop(columns='geometry').copy(), geometry='geometry_buffered')
    return dataframe_stops_buffer

'''
Fonction: Filtrer les services dans un geodataframe des dates de calendrier par jour de la semaine voulu
Entrée: Un dataframe de dates de calendrier et une liste de jours de la semaine en français
Sortie: Un dataframe des services filtrés par les jours de la semaine voulus
'''
def filter_service_by_days_of_week(dataframe_calendar_dates, list_days_of_week):
    # Add a column with the day of the week in English
    dataframe_calendar_dates['day_of_week'] = dataframe_calendar_dates['date'].dt.day_name()
    # Translate the day of the week to French
    dataframe_calendar_dates['day_of_week'] = dataframe_calendar_dates['day_of_week'].replace({
        'Monday': 'Lundi', 'Tuesday': 'Mardi', 'Wednesday': 'Mercredi',
        'Thursday': 'Jeudi', 'Friday': 'Vendredi', 'Saturday': 'Samedi', 'Sunday': 'Dimanche'
    })
    # Filter for the specified days of the week
    dataframe_calendar_dates_filtered = dataframe_calendar_dates[dataframe_calendar_dates['day_of_week'].isin(list_days_of_week)]
    dataframe_calendar_dates_filtered = dataframe_calendar_dates_filtered['service_id'].unique()
    return dataframe_calendar_dates_filtered


'''
Fonction: Filter les arrêts selon une plage horaire donnée d'heure de début et fin donnée
Entrée: Un dataframe des horaires des arrêts, une heure de début et une heure de fin
Sortie: Un dataframe des horaires des arrêts filtrés selon la plage horaire
'''
def filter_stops_by_time(dataframe_stop_times, heure_debut, heure_fin):
    heure_min = pd.to_timedelta(heure_debut)
    heure_max = pd.to_timedelta(heure_fin)
    dataframe_stop_times.loc[:,'departure_time'] = pd.to_timedelta(dataframe_stop_times['departure_time'])
    dataframe_stop_times.loc[:,'departure_time'] = dataframe_stop_times['departure_time'].astype(str).str.split().str[-1]
    dataframe_stop_times.loc[:,'departure_time'] = pd.to_timedelta(dataframe_stop_times['departure_time'])
    dataframe_stop_times_filtered = dataframe_stop_times[(dataframe_stop_times['departure_time'] >= heure_min) & (dataframe_stop_times['departure_time'] <= heure_max)]
    return dataframe_stop_times_filtered



# IMPORTATION DES DONNÉES
# Création du chemin de base pour tous les fichiers

# Sarah
base_path = r'C:\Users\sarah\OneDrive\UDES\AUT2024\GMQ710\TD2\data_td2\data'

#Justin
# base_path = r'D:\Utilisateur\OneDrive - USherbrooke\Analyse_programmation\TD2\data_td2\data'

# Chemin des fichiers dans le dossier address
address_filepath = rf'{base_path}\address\Adresses.geojson'

# Chemin des fichiers dans le dossier stats
sherbrooke_ilot_pop_filepath = rf'{base_path}\stats\sherbrooke_ilot_pop.csv'
sherbrooke_ilots_filepath = rf'{base_path}\stats\sherbrooke_ilots.shp'

# Chemin des fichier dans le dossier sts
calendar_dates_filepath = rf'{base_path}\sts\calendar_dates.txt'
routes_filepath = rf'{base_path}\sts\routes.txt'
shapes_filepath = rf'{base_path}\sts\shapes.txt'
stop_times_filepath = rf'{base_path}\sts\stop_times.txt'
stops_filepath = rf'{base_path}\sts\stops.txt'
trips_filepath = rf'{base_path}\sts\trips.txt'

# Lecture des fichiers
gdf_address = gpd.read_file(address_filepath)
df_sherbrooke_ilot_pop = pd.read_csv(sherbrooke_ilot_pop_filepath)
gdf_sherbrooke_ilots = gpd.read_file(sherbrooke_ilots_filepath)
df_calendar_dates = pd.read_csv(calendar_dates_filepath)
df_routes = pd.read_csv(routes_filepath)
df_shapes = pd.read_csv(shapes_filepath)
df_stop_times = pd.read_csv(stop_times_filepath)
df_stops = pd.read_csv(stops_filepath)
df_trips = pd.read_csv(trips_filepath)

# #Affichage des données
# print('---------------------------------')
# print('Calendar dates')
# print(df_calendar_dates.head())
# print('---------------------------------')
# print('Routes')
# print(df_routes.head())
# print('---------------------------------')
# print('Shapes')
# print(df_shapes.head())
# print('---------------------------------')
# print('Stop times')
# print(df_stop_times.head())
# print('---------------------------------')
# print('Stops')
# print(df_stops.head())
# print('---------------------------------')
# print('Trips')
# print(df_trips.head())

# PRÉPARATION DES DONNÉES


# Filtrer les routes pour l'agence STS
df_routes = df_routes[df_routes['agency_id'] == 0]
# Filtrer les trips pour l'agence STS
df_trips = df_trips[df_trips['route_id'].isin(df_routes['route_id'])]
# Filtrer les stops pour l'agence STS
df_stop_times = df_stop_times[df_stop_times['trip_id'].isin(df_trips['trip_id'])]
# Filtrer les stops pour l'agence STS
df_stops = df_stops[df_stops['stop_id'].isin(df_stop_times['stop_id'])]
# Filtrer calendar dates pour l'agence STS
dataframe_calendar_dates = df_calendar_dates[df_calendar_dates['service_id'].isin(df_trips['service_id'])]
# Filtrer shapes pour l'agence STS
dataframe_shapes = df_shapes[df_shapes['shape_id'].isin(df_trips['shape_id'])]

# DF_STOPS : Dataframe -> Geodataframe pour stops en points
gdf_stops = gpd.GeoDataFrame(df_stops, geometry=gpd.points_from_xy(df_stops.stop_lon, df_stops.stop_lat))
# Set CRS à WGS84 (EPSG:4326)
gdf_stops.set_crs(epsg=crs, inplace=True)
# Projection en UTM zone 18N (EPSG:32618) pour avoir des unités en mètres
gdf_stops = gdf_stops.to_crs(epsg=projection)
# print(gdf_stops)

# DF_SHAPES : Dataframe -> Geodataframe pour shapes en linestring
lines_for_shapes = []
for shape_id in pd.unique(df_shapes['shape_id']):
    df_shape = df_shapes[df_shapes['shape_id'] == shape_id]
    points = [Point(xy) for xy in zip(df_shape.shape_pt_lon, df_shape.shape_pt_lat)]
    line = LineString(points)
    lines_for_shapes.append({'shape_id': shape_id, 'geometry': line})
gdf_shapes = gpd.GeoDataFrame(lines_for_shapes)
# Ajuster CRS à WGS84 (EPSG:4326) et la projection en UTM 18N (EPSG:32618)
gdf_shapes = set_crs_proj(gdf_shapes, crs, projection)

# DF_TRIPS : Création des lignes du bus avec gdf_shapes
df_trips_geom = df_trips.merge(gdf_shapes[['shape_id', 'geometry']], on = 'shape_id', how= 'left')
gdf_trips_geom = gpd.GeoDataFrame(df_trips_geom, geometry='geometry')
# print(gdf_trips_geom)
# Lignes de bus sont tous les trips regroupés par route_id
ligne_bus = gdf_trips_geom.dissolve(by='route_id')
# print(ligne_bus)

# DF_STOP_TIMES : Convertir la colonne departure_time en timedelta
df_stop_times['departure_time'] = pd.to_timedelta(df_stop_times['departure_time'])
df_stop_times['departure_time'] = df_stop_times['departure_time'] % pd.Timedelta(days=1)
# print(df_stop_times)

# DF_CALENDAR_DATES : Convertir la colonne date en datetime
df_calendar_dates['date'] = pd.to_datetime(df_calendar_dates['date'], format='%Y%m%d')

# GDF_ADDRESS : Ajuster CRS à WGS84 (EPSG:4326) et la projection en UTM 18N (EPSG:32618)
gdf_address = set_crs_proj(gdf_address, crs, projection)

# GDF_SHERBROOKE_ILOTS : Ajuster CRS à WGS84 (EPSG:4326) et la projection en UTM 18N (EPSG:32618)
gdf_sherbrooke_ilots = set_crs_proj(gdf_sherbrooke_ilots, crs, projection)


# # ===============================================================

# ### ANALYSE DES DONNÉES

# ----- Que représente le réseau de la STS ? notamment -----
# QUESTION: Nombre d'arrêts dans la ville
nb_stops = len(pd.unique(df_stops['stop_id']))
print('Nombre de stops: ', nb_stops)
print('------------------------')
# ----------

# QUESTION: Nombre de lignes
nb_lignes = len(pd.unique(df_routes['route_id']))
print('Nombre de lignes: ', nb_lignes)
print('------------------------')
# ----------

# QUESTION: Dates de début et de fin du calendrier
# Conversion en format datetime de la colonne date et affichage des dates de début et de fin
df_calendar_dates['date'] = pd.to_datetime(df_calendar_dates['date'], format='%Y%m%d')
print('Date de début: ', df_calendar_dates['date'].min())
print('Date de fin: ', df_calendar_dates['date'].max())
print('------------------------')
# ----------

# QUESTION: Calcul de longueur totale de tous les linestring en kilomètres
# Calcul et affichage
gdf_shapes['length_km'] = (gdf_shapes.length)/1000
print('Total length in kilometers:', round(gdf_shapes['length_km'].sum()), 'km')

# ***************************************************************************************
## ----- Existe-t-il des endroits à surveiller sur le réseau -----

# QUESTION 1: Pour les journées en semaine, pour la plage entre 11 h et 13 h, quels sont les 5 arrêts les plus populaires ?
# Filtrage des dates pour les jours de semaine
df_calendar_dates_filtered = filter_service_by_days_of_week(df_calendar_dates, days_of_week)

# Filtrage des trips pour les jours de semaine
df_trips_semaine = df_trips[df_trips['service_id'].isin(df_calendar_dates_filtered)]
# print(df_trips_semaine) # On obtient affichage des trips pour les jours de semaine pour l'agence STS

# Filtrage des stop_times pour les trips en semaine
df_stop_times_semaine = df_stop_times[df_stop_times['trip_id'].isin(df_trips_semaine['trip_id'])]
# print(df_stop_times_semaine) # On obtient affichage des stop_times pour les trips sts en semaine

# Filtrage des stop_times pour la plage horaire
df_stop_times_plage_horaire = filter_stops_by_time(df_stop_times_semaine, test_heure_debut, test_heure_fin)

# Comptage des arrêts dans la plage horaire en ordre du plus grand passage
df_stop_times_count = df_stop_times_plage_horaire['stop_id'].value_counts().reset_index()
df_stop_times_count5 = df_stop_times_count.sort_values(by= 'count', ascending=False)[:5]
# print(df_stop_times_count5)

# add column of 'stop_name' from df_stops to df_topX_stops
df_stop_times_count5 = df_stop_times_count5.merge(df_stops[['stop_id', 'stop_name']], left_on='stop_id', right_on='stop_id')
# print(df_stop_times_count5)

# Affichage des arrêts les plus populaires
print('Les arrêts les plus populaires entre', test_heure_debut, 'et ', test_heure_fin,' sont les suivants :')
for stop_id, stop_name, count in zip(df_stop_times_count5['stop_id'], df_stop_times_count5['stop_name'], df_stop_times_count5['count']):
    print('# Arrêt :', stop_id, '|| Nom :', stop_name, '|| Nombre de passages :', count)
print('------------------------')

# # ***************************************************************************************
# Question 2 
# Pour les journées en semaine, pour la plage entre 11 h et 13 h, combien d'adresses sont à proximité d'un arrêt fréquenté durant cette période ?
# Filtrage des dates pour les jours de semaine
df_calendar_dates_filtered = filter_service_by_days_of_week(df_calendar_dates, days_of_week)

# Filtrage des trips pour les jours de semaine
df_trips_semaine = df_trips[df_trips['service_id'].isin(df_calendar_dates_filtered)]
# print(df_trips_semaine) # On obtient affichage des trips pour les jours de semaine pour l'agence STS

# Filtrage des stop_times pour les trips en semaine
df_stop_times_semaine = df_stop_times[df_stop_times['trip_id'].isin(df_trips_semaine['trip_id'])]
# print(df_stop_times_semaine) # On obtient affichage des stop_times pour les trips sts en semaine

# Filtrage des stop_times pour la plage horaire
df_stop_times_plage_horaire = filter_stops_by_time(df_stop_times_semaine, test_heure_debut, test_heure_fin)

# Filtrage des stops selon stop_times_plage_horaire
gdf_stops_plage_horaire = gdf_stops[gdf_stops['stop_id'].isin(df_stop_times_plage_horaire['stop_id'])].reset_index()
# Création de buffer autour des points gdf_stops_plage_horaire
gdf_stops_buffer = creation_buffer_autour_arrets(gdf_stops_plage_horaire, proximite_arret)

# Jointure spatiale entre les adresses et les arrêts fréquentés
gdf_address_near_stops = gpd.sjoin(gdf_address, gdf_stops_buffer, how='inner', predicate='intersects')

# Compter les adresses uniques proche des arrêts fréquentés dans la plage horaire
nb_adresses_uniques = gdf_address_near_stops['ADRESSE'].nunique()
print('Nombre d\'adresses à proximité des arrêts fréquentés :', nb_adresses_uniques)

print('------------------------')

# ***************************************************************************************
# Question 3.
# En utilisant une adresse de la Ville (ex : 4289 rue Martin), une date et une heure, peut-on connaître les services de la STS qui passent à proximité ?


# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
# Création de buffer autour de adresse
gdf_address_filtered_buffer = creation_buffer_autour_adresse(gdf_address, adresse, proximite_arret)
# Trouver arrêts à l'intérieur du buffer
gdf_stops_inside_buffer = gdf_stops[gdf_stops.geometry.within(gdf_address_filtered_buffer.union_all())]
# Détermination des marges d'heures selon l'heure et la marge
test_heure_min, test_heure_max = creation_marge_temps(heure, marge)
# # Trouver les horaires qui passent à ces arrêts pour l'heure
df_stop_times_filtered = df_stop_times[(df_stop_times['departure_time'] >= test_heure_min) & (df_stop_times['departure_time'] <= test_heure_max) & (df_stop_times['stop_id'].isin(gdf_stops_inside_buffer['stop_id']))]
# Trouver le service de la date dans calendar_dates
date = pd.to_datetime(date, format='%Y%m%d')
df_calendar_dates_filtered = df_calendar_dates[df_calendar_dates['date'] == date]
# Trouver les trips associés aux horaires et aux services
gdf_trips_filtered = ligne_bus[(ligne_bus['trip_id'].isin(df_stop_times_filtered['trip_id'])) & (ligne_bus['service_id'].isin(df_calendar_dates_filtered['service_id']))]

# Merge trips pour avoir 'route_id' dans gdf_trips_filtered
if 'route_id' not in gdf_trips_filtered.columns:
    gdf_trips_filtered = gdf_trips_filtered.merge(df_trips[['trip_id', 'route_id']], on='trip_id')

# Merge stops avec df_stop_times pour avoir les noms des arrêts
df_stop_times_filtered = df_stop_times_filtered.merge(gdf_stops[['stop_id', 'stop_name']], on='stop_id')
# Merge stop times avec df_trips pour avoir les noms des stops
gdf_trips_filtered = gdf_trips_filtered.merge(df_stop_times_filtered[['trip_id', 'stop_id', 'stop_name']], on='trip_id')
# Merge trips avec df_routes pour avoir les noms des lignes
gdf_trips_filtered = gdf_trips_filtered.merge(df_routes[['route_id', 'route_long_name']], on='route_id')
# Sort by stop_id ensuite route_id
gdf_trips_filtered = gdf_trips_filtered.sort_values(by=['stop_id', 'route_id'])

# print columns of gdf_trips_filtered
# print(gdf_trips_filtered.columns)


# Reponse à la question
print('Pour l\'adresse', adresse, 'à la date', date, 'et l\'heure', heure, ' avec un marge de', marge, 'les services de la STS qui passent à proximité sont :')
for stop_id, stop_name, route_id, route_long_name, service_id, trip_id in zip(gdf_trips_filtered['stop_id'], gdf_trips_filtered['stop_name'], gdf_trips_filtered['route_id'], gdf_trips_filtered['route_long_name'], gdf_trips_filtered['service_id'], gdf_trips_filtered['trip_id']):
    print('# Stop :', stop_id,'Nom du stop :', stop_name,'# Ligne :', route_id, '|| Nom de ligne :', route_long_name, '|| # Service :', service_id, '|| # Trip :', trip_id)
print('------------------------')


# xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx


# # ***************************************************************************************
# Question 4.
# Si on utilise l’empreinte des circuits de ligne de bus de la STS et les îlots, peut-on connaître le
# circuit de bus qui couvrent le plus de personnes ? (on pourrait vérifier les îlots qui sont
# traversés par une ligne et en déduire les populations)

# 1.	Jointure d’attribut selon colonne DBUID_IDIDU de la table sherbrooke_ilots_pop.csv avec sherbrooke_ilots.shp
gdf_sherbrooke_ilots['IDIDU'] = gdf_sherbrooke_ilots['IDIDU'].astype(str)
df_sherbrooke_ilot_pop['DBUID_IDIDU'] = df_sherbrooke_ilot_pop['DBUID_IDIDU'].astype(str)

gdf_ilot_merge = gdf_sherbrooke_ilots.merge(df_sherbrooke_ilot_pop, left_on = 'IDIDU', right_on = "DBUID_IDIDU")
# Reconversion en gdf
# gdf_ilot_merge = gpd.GeoDataFrame(ilot_merge, geometry= 'geometry')
# gdf_ilot_merge = gdf_ilot_merge.to_crs(crs= projection)


# 2. Intersect gdf_lignes avec gdf_sherbrooke_ilots.shp
ilot_intersect = gpd.sjoin(gdf_ilot_merge, ligne_bus, how='inner', predicate='intersects')
# print(ilot_intersect.columns)
# Attribuer la population total à chaque ligne d'autobus en groupant par trip_id
pop_by_circuit = ilot_intersect.groupby('trip_id')['DBPOP2021_IDPOP2021'].sum().reset_index()
pop_by_circuit_sorted = pop_by_circuit.sort_values(by='DBPOP2021_IDPOP2021', ascending=False)
# Ajout de la colonne route_id pour avoir le nom de la ligne selon le circuit
pop_by_circuit_sorted_merge_ligne = pop_by_circuit_sorted.merge(df_trips[['trip_id', 'route_id']], left_on='trip_id', right_on='trip_id')
pop_by_circuit_sorted_merge_ligne['DBPOP2021_IDPOP2021'] = pop_by_circuit_sorted_merge_ligne['DBPOP2021_IDPOP2021'].astype(int)
pop_by_circuit_sorted_merge_ligne['route_id'] = pop_by_circuit_sorted_merge_ligne['route_id'].astype(str)
# print(pop_by_circuit_sorted_merge_ligne)
circuit_populaire = pop_by_circuit_sorted_merge_ligne.iloc[0]


# Afficher les résultats
print(f"Le circuit qui touche le plus de population est le circuit {circuit_populaire['trip_id']} emprunté par la ligne de bus {circuit_populaire['route_id']} "f"avec une population totale de {circuit_populaire['DBPOP2021_IDPOP2021']}.")
print('------------------------')

# ***************************************************************************************
# 5.Peut-on connaître les 5 journées dans l’année où la longueur totale des lignes de bus sont les
# plus faibles ? (en fait on souhaite savoir combien de km ont été couverts chaque jour et trouver
# les journées les plus faibles en distance)
# Calcul de la longueur totale des lignes de bus
gdf_trips_geom['longueur'] = gdf_trips_geom.length
# Groupby par service_id pour avoir la longueur totale de chaque service
longueur_service = gdf_trips_geom.groupby('service_id')['longueur'].sum().reset_index()
# Extraire les services avec la longueur totale la plus faible
longueur_service_min = longueur_service[longueur_service['longueur'] == longueur_service['longueur'].min()]
# Extraire les dates ayant la plus petite longueur
date_longueur_service_min = df_calendar_dates[df_calendar_dates['service_id'].isin(longueur_service_min['service_id'])]
# Créer une copie de dates_petit pour convertir la colonne date en datetime
date_longueur_service_min_copy = date_longueur_service_min.copy()
# Convertir la colonne date en datetime
date_longueur_service_min_copy['date'] = pd.to_datetime(date_longueur_service_min_copy['date'], format='%Y%m%d')
# Garder seulement top 5 des dates
top5_date_longueur_min = date_longueur_service_min_copy[:5]

print('Les 5 journées où la longueur totale sont les plus faibles, '
      'soit de {:.2f} kilomètres :'.format(longueur_service_min['longueur'].values[0] / 1000))
for date in top5_date_longueur_min['date']:
    print(date.strftime('%Y-%m-%d'))
print('------------------------')
# ***************************************************************************************
# 6. Pourriez-vous générer un fichier GeoJson de l’image du réseau pour la plage 11 h à 13 h un mercredi ?

# Filtrer calendar dates pour garder seulement le mercredi
df_calendar_dates_mercredi = filter_service_by_days_of_week(df_calendar_dates, day_of_week)


# Filtrer stops_times pour garder les trips qui sont dans la plage horaire
df_stop_times_filtered = filter_stops_by_time(df_stop_times, test_heure_debut, test_heure_fin)

# Filtrer trips pour garder les trips qui sont dans la plage horaire et le mercredi
df_trips_filtered = gdf_trips_geom[gdf_trips_geom['service_id'].isin(df_calendar_dates_mercredi) & gdf_trips_geom['trip_id'].isin(df_stop_times_filtered['trip_id'])]
print(df_trips_filtered)
# Exporter les données en GeoJson
df_trips_filtered.to_file(rf'{base_path}\network_image2.geojson', driver='GeoJSON')
print('Fichier network_image2.geojson créé')




    