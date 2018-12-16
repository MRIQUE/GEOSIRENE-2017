# GEOSIRENE-2017
<b>Téléchargement et insertion dans une table sql serveur de  la base SIRENE Geocodée VERSION 2017</b><br>
Christian Quest gère le géocodage de la base SIREN des entreprises françaises publiée en open data et la met à disposition sur un repository à cette adresse
http://data.cquest.org/geo_sirene/last/ <br>
Ce développement permet d'intégrer ce référentiel fournis online en csv zippé dans une table sql serveur afin de réaliser facilement des traitements
<hr>
create_table Geo_sirene.txt => Instruction sql pour créer la table réceptacle (sans les index)
<br>
Code source en c# 
<hr>


a BD SIRENE géocodée a été téléchargée Celle-ci a été géocodée par Christian Quest à partir de la BAN (Base Adresse Nationale) et la BANO (Base Adresse Nationale Ouverte). Le csv téléchargé comporte plus de 10 millions d'entitéés, et 91 colonnes : les 84 de la BD SIRENE originale + 7 ajoutées avec le géocodage, dont la latitude et la longitude en EPSG 4326). Le script python relancé chaque mois pour réactualiser la base est disponible sur son github Les colonnes de libellés de la BD SIRENE originale contenant de longues chaînes de caractères ont été éliminées pour gagner en mémoire. Celles concernant la nomenclature de d'activités françaises (NAF) seront réintégrées dans l'extrait de la table utilisé grâce aux tables NAF, placées dans la base de données.
Merci à Christian pour sa contribution permanente intelligente et utile !
