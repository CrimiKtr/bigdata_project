"""
src/03_verify.py : vérification indépendante des résultats du pipeline
Ajouté au commit 7 (samedi 28 novembre 2026, Membre B)

Trois contrôles :
  1. DEUXIÈME MÉTHODE : on recalcule l'IPS moyen par année / académie / secteur
     directement depuis le CSV BRUT, en SQL avec DuckDB (sans utiliser le code pandas
     du pipeline), et on compare avec le Parquet produit dans processed/.
  2. CONTRÔLE MANUEL : on affiche une ligne brute et la ligne nettoyée d'un même collège.
  3. CHIFFRE OFFICIEL (facultatif) : comparaison avec un IPS moyen national publié par
     la DEPP, à saisir dans la variable OFFICIEL ci-dessous.

Utilisation :
  export S3_BUCKET=user-<identifiant>     (ex. user-t-ince-ece)
  python src/03_verify.py
"""

import json
import os
import re
import unicodedata
from datetime import datetime

import duckdb
import pandas as pd

# Accès S3 commun à tous les scripts (clés lues dans ~/.aws/credentials ou l'environnement)
from s3_utils import afficher_contexte, ouvrir_stockage

IPS_MIN, IPS_MAX = 30, 200   # mêmes bornes que le pipeline (règle métier documentée)
TOLERANCE = 0.01             # les moyennes du pipeline sont arrondies à 0,01

# Chiffre officiel à compléter à la main (source : note d'information DEPP).
# Exemple : OFFICIEL = {"annee": 2021, "secteur": "PUBLIC", "valeur": 0.0, "source": "..."}
OFFICIEL = None


def normaliser_nom(nom):
    nom = unicodedata.normalize("NFKD", str(nom)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", nom.lower()).strip("_")


def main():
    fs, base = ouvrir_stockage()
    afficher_contexte(base)

    # --- Copie locale du CSV brut (DuckDB le lit ensuite directement) ---------------
    local_csv = "/tmp/ips_brut_verif.csv"
    fs.get(f"{base}/raw/fr-en-ips_colleges.csv", local_csv)
    with open(local_csv, encoding="utf-8-sig", errors="replace") as f:
        entete = f.readline().rstrip("\n\r")
    sep = ";" if entete.count(";") > entete.count(",") else ","
    noms = [normaliser_nom(c) for c in entete.split(sep)]

    def col(motif, exclure=None):
        if motif == "ips" and "ips" in noms:
            return "ips"
        return next(c for c in noms if motif in c and not (exclure and exclure in c))

    c_rentree, c_acad, c_sect, c_uai, c_ips = col("rentree"), col("academie"), col("secteur"), col("uai"), col("ips", "ecart")

    # --- 1. Deuxième méthode : tout en SQL avec DuckDB ------------------------------
    sql = f"""
    WITH brut AS (
        SELECT *, row_number() OVER () AS num_ligne
        FROM read_csv('{local_csv}', delim='{sep}', header=true, all_varchar=true, names={noms})
    ),
    nettoye AS (
        SELECT DISTINCT ON (trim({c_uai}), trim({c_rentree}))
            trim({c_uai}) AS uai,
            upper(trim({c_acad})) AS academie,
            TRY_CAST(regexp_extract({c_rentree}, '(\\d{{4}})', 1) AS INTEGER) AS annee,
            CASE WHEN lower(strip_accents(trim({c_sect}))) LIKE 'pub%' THEN 'PUBLIC'
                 WHEN lower(strip_accents(trim({c_sect}))) LIKE 'priv%' THEN 'PRIVE' END AS secteur,
            TRY_CAST(replace(trim({c_ips}), ',', '.') AS DOUBLE) AS ips
        FROM brut
        ORDER BY trim({c_uai}), trim({c_rentree}), num_ligne
    )
    SELECT annee, academie, secteur, count(*) AS nb_colleges_sql, avg(ips) AS ips_moyen_sql
    FROM nettoye
    WHERE ips IS NOT NULL AND annee IS NOT NULL AND secteur IS NOT NULL
      AND ips BETWEEN {IPS_MIN} AND {IPS_MAX}
    GROUP BY ALL
    """
    sql_res = duckdb.sql(sql).df()

    with fs.open(f"{base}/processed/ips_academie_secteur.parquet", "rb") as f:
        pipe = pd.read_parquet(f)

    comp = pipe.merge(sql_res, on=["annee", "academie", "secteur"], how="outer", indicator=True)
    groupes_absents = int((comp["_merge"] != "both").sum())
    comp = comp[comp["_merge"] == "both"]
    ecart_max = float((comp["ips_moyen"] - comp["ips_moyen_sql"]).abs().max())
    comptes_differents = int((comp["nb_colleges"] != comp["nb_colleges_sql"]).sum())
    ok = groupes_absents == 0 and comptes_differents == 0 and ecart_max <= TOLERANCE

    print("\n=== 1. Pipeline pandas contre recalcul SQL DuckDB depuis le brut ===")
    print("Groupes comparés                 :", len(comp))
    print("Groupes présents d'un seul côté  :", groupes_absents)
    print("Groupes avec un nombre différent :", comptes_differents)
    print(f"Écart maximal sur l'IPS moyen    : {ecart_max:.4f} (tolérance {TOLERANCE})")
    print("RÉSULTAT                         :", "OK, les deux méthodes concordent" if ok else "ÉCHEC, à analyser")

    # --- 2. Contrôle manuel sur un collège -----------------------------------------
    with fs.open(f"{base}/processed/ips_colleges_clean.parquet", "rb") as f:
        propre = pd.read_parquet(f, columns=["uai", "annee", "academie", "secteur", "ips"])
    exemple = propre.iloc[len(propre) // 2]
    print("\n=== 2. Contrôle manuel sur un collège ===")
    print("Ligne nettoyée :", exemple.to_dict())
    brut_ex = duckdb.sql(f"""SELECT * FROM read_csv('{local_csv}', delim='{sep}', header=true,
                             all_varchar=true, names={noms})
                             WHERE trim({c_uai}) = '{exemple['uai']}'""").df()
    print("Ligne(s) brute(s) du même UAI :")
    print(brut_ex.to_string(index=False))

    # --- 3. Comparaison avec un chiffre officiel (si renseigné) ---------------------
    resultat_officiel = None
    if OFFICIEL:
        sel = propre[(propre["annee"] == OFFICIEL["annee"]) & (propre["secteur"] == OFFICIEL["secteur"])]
        calcule = round(float(sel["ips"].mean()), 2)
        resultat_officiel = {**OFFICIEL, "valeur_calculee_moyenne_simple": calcule}
        print("\n=== 3. Comparaison avec le chiffre officiel ===")
        print(f"Officiel : {OFFICIEL['valeur']} | Calculé (moyenne simple des collèges) : {calcule}")
        print("Remarque : la DEPP pondère par le nombre d'élèves, un petit écart est donc attendu.")

    with fs.open(f"{base}/processed/verification.json", "w") as f:
        json.dump({"date": datetime.now().isoformat(timespec="seconds"), "groupes_compares": len(comp),
                   "groupes_absents": groupes_absents, "comptes_differents": comptes_differents,
                   "ecart_max": ecart_max, "ok": ok, "officiel": resultat_officiel}, f, indent=2)
    print("\nRésultat enregistré dans processed/verification.json")


if __name__ == "__main__":
    main()
