"""
src/04_benchmark.py : mesure comparative pandas contre DuckDB
Ajouté au commit 8 (samedi 28 novembre 2026, Membre B)

Même traitement pour les deux outils : lecture du CSV brut, nettoyage minimal
(espaces, IPS en nombre, secteur harmonisé, doublons de clé), puis IPS moyen par
année / académie / secteur. Chaque mesure est répétée REPETITIONS fois ; on garde
la médiane. Pour estimer l'effet d'un volume plus gros, le CSV est recopié
k fois (facteurs 1, 10 ... ) : les doublons de clé sont alors supprimés par le
traitement, ce qui reste représentatif du coût de lecture et de dédoublonnage.

Utilisation :
  export S3_BUCKET=user-<identifiant>     (ex. user-t-ince-ece)
  python src/04_benchmark.py
  # facultatif, avec un service d'au moins 8 Go de RAM :
  BENCH_FACTEURS=1,10,100 python src/04_benchmark.py
"""

import os
import re
import statistics
import time
import unicodedata
from datetime import datetime

import duckdb
import pandas as pd

# Accès S3 commun à tous les scripts (clés lues dans ~/.aws/credentials ou l'environnement)
from s3_utils import afficher_contexte, ouvrir_stockage

REPETITIONS = 5
FACTEURS = [int(x) for x in os.environ.get("BENCH_FACTEURS", "1,10").split(",")]


def normaliser_nom(nom):
    nom = unicodedata.normalize("NFKD", str(nom)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", nom.lower()).strip("_")


def preparer_fichier(source, facteur):
    """Crée un CSV contenant `facteur` copies des lignes de données (en-tête unique)."""
    if facteur == 1:
        return source
    cible = f"/tmp/ips_x{facteur}.csv"
    with open(source, encoding="utf-8-sig") as f:
        entete, corps = f.readline(), f.read()
    if not corps.endswith("\n"):
        corps += "\n"
    with open(cible, "w", encoding="utf-8") as f:
        f.write(entete)
        for _ in range(facteur):
            f.write(corps)
    return cible


def traitement_pandas(chemin, sep, cols):
    df = pd.read_csv(chemin, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = [normaliser_nom(c) for c in df.columns]
    df = df[[cols["uai"], cols["rentree"], cols["academie"], cols["secteur"], cols["ips"]]].apply(lambda s: s.str.strip())
    df = df.drop_duplicates(subset=[cols["uai"], cols["rentree"]])
    df["ips_num"] = pd.to_numeric(df[cols["ips"]].str.replace(",", ".", regex=False), errors="coerce")
    df["annee"] = df[cols["rentree"]].str[:4]
    df["secteur_n"] = df[cols["secteur"]].str.lower().str.startswith("pub").map({True: "PUBLIC", False: "PRIVE"})
    return df.dropna(subset=["ips_num"]).groupby(["annee", cols["academie"], "secteur_n"])["ips_num"].mean()


def traitement_duckdb(chemin, sep, cols, noms):
    return duckdb.sql(f"""
        WITH t AS (
            SELECT DISTINCT ON (trim({cols['uai']}), trim({cols['rentree']}))
                   left(trim({cols['rentree']}), 4) AS annee, trim({cols['academie']}) AS academie,
                   CASE WHEN lower(trim({cols['secteur']})) LIKE 'pub%' THEN 'PUBLIC' ELSE 'PRIVE' END AS secteur,
                   TRY_CAST(replace(trim({cols['ips']}), ',', '.') AS DOUBLE) AS ips
            FROM read_csv('{chemin}', delim='{sep}', header=true, all_varchar=true, names={noms})
        )
        SELECT annee, academie, secteur, avg(ips) FROM t WHERE ips IS NOT NULL GROUP BY ALL
    """).df()


def mesurer(fonction):
    durees = []
    for _ in range(REPETITIONS):
        t0 = time.perf_counter()
        fonction()
        durees.append(time.perf_counter() - t0)
    return statistics.median(durees)


def main():
    fs, base = ouvrir_stockage()
    afficher_contexte(base)
    print("CPU disponibles  :", os.cpu_count())

    source = "/tmp/ips_brut_bench.csv"
    fs.get(f"{base}/raw/fr-en-ips_colleges.csv", source)
    with open(source, encoding="utf-8-sig") as f:
        entete = f.readline().rstrip("\n\r")
    sep = ";" if entete.count(";") > entete.count(",") else ","
    noms = [normaliser_nom(c) for c in entete.split(sep)]

    def trouver(motif, exclure=None):
        if motif == "ips" and "ips" in noms:
            return "ips"
        return next(c for c in noms if motif in c and not (exclure and exclure in c))

    cols = {"uai": trouver("uai"), "rentree": trouver("rentree"), "academie": trouver("academie"),
            "secteur": trouver("secteur"), "ips": trouver("ips", "ecart")}

    resultats = []
    for k in FACTEURS:
        chemin = preparer_fichier(source, k)
        taille_mo = os.path.getsize(chemin) / 1e6
        t_pandas = mesurer(lambda: traitement_pandas(chemin, sep, cols))
        t_duckdb = mesurer(lambda: traitement_duckdb(chemin, sep, cols, noms))
        resultats.append({"facteur": k, "taille_mo": round(taille_mo, 1),
                          "pandas_s": round(t_pandas, 3), "duckdb_s": round(t_duckdb, 3),
                          "rapport_pandas_sur_duckdb": round(t_pandas / t_duckdb, 2)})
        print(f"x{k:<4} {taille_mo:8.1f} Mo | pandas {t_pandas:7.3f} s | duckdb {t_duckdb:7.3f} s")

    tableau = pd.DataFrame(resultats)
    print(f"\n=== Médiane de {REPETITIONS} exécutions ===")
    print(tableau.to_string(index=False))
    with fs.open(f"{base}/processed/benchmark_resultats.csv", "w") as f:
        tableau.to_csv(f, index=False)
    print("\nRésultats enregistrés dans processed/benchmark_resultats.csv")


if __name__ == "__main__":
    main()
