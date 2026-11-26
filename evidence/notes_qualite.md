# Notes sur la qualité des données

Source des chiffres : exécution de `python src/02_pipeline.py` le 26/11/2026 à 11 h 48 sur Onyxia
(namespace `user-t-ince-ece`), lecture de `s3://user-t-ince-ece/bigdata_project/raw/fr-en-ips_colleges.csv`.
Capture associée : `evidence/02_execution_nettoyage.png`.

## Vue d'ensemble

- 41 701 lignes brutes (une ligne = un collège pour une rentrée), 11 colonnes, séparateur `;`.
- 6 rentrées : de 2016-2017 à 2021-2022, entre 6 935 et 6 962 collèges par rentrée.
- 41 701 lignes conservées après nettoyage : **aucune ligne supprimée**.

## Contrôles effectués

| Contrôle | Lignes concernées | Traitement |
|---|---|---|
| Espaces en début ou fin de texte | 0 | `strip()` sur toutes les colonnes (sans effet ici) |
| Doublons exacts | 0 | suppression prévue (rien à supprimer) |
| Même collège (UAI) deux fois pour une même rentrée | 0 | première ligne conservée (rien à supprimer) |
| IPS manquant ou non numérique | 0 | ligne écartée (rien à écarter) |
| IPS avec virgule décimale | 0 | virgule remplacée par un point (sans effet ici) |
| IPS hors bornes 30-200 | 0 | ligne écartée ; IPS observé entre 47,5 et 157,6 |
| Rentrée illisible | 0 | ligne écartée (rien à écarter) |
| Secteur non reconnu | 0 | libellés « public » (31 752) et « privé sous contrat » (9 949) harmonisés en PUBLIC / PRIVE |
| **Écart-type de l'IPS manquant** | **20 840** | **colonne conservée mais non utilisée** |

## Le vrai problème : l'écart-type de l'IPS

La colonne « Ecart-type de l'IPS » est vide pour 20 840 lignes sur 41 701 (50 %).
Ce n'est pas aléatoire : elle est vide pour **tous** les collèges des trois premières rentrées,
et remplie pour **tous** les collèges des trois suivantes.

| Rentrée | Écart-type manquant | Collèges |
|---|---|---|
| 2016-2017 | 6 935 | 6 935 |
| 2017-2018 | 6 954 | 6 954 |
| 2018-2019 | 6 951 | 6 951 |
| 2019-2020 | 0 | 6 946 |
| 2020-2021 | 0 | 6 953 |
| 2021-2022 | 0 | 6 962 |

Répartition par secteur des lignes concernées : 15 866 publics, 4 974 privés sous contrat.

Exemples réels :
- manquant : rentrée 2016-2017, UAI `0010080G` (privé sous contrat), IPS 105,0, écart-type vide ;
- renseigné : rentrée 2019-2020, UAI `0952145R`, IPS 115,5, écart-type 31,0.

Décision : notre question porte sur l'IPS moyen, pas sur sa dispersion. Supprimer ces lignes ferait
perdre la moitié de la période étudiée ; on garde donc toutes les lignes et on n'utilise pas l'écart-type.
Conséquence à signaler dans le rapport : on ne peut pas comparer la dispersion sociale interne des collèges
avant 2019.

Pour retrouver un exemple dans le fichier brut :
`grep "0010080G" /tmp/bigdata_project_raw/fr-en-ips_colleges.csv`
