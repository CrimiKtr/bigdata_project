"""
src/02_pipeline.py (VERSION DU COMMIT 4) : lecture et nettoyage des IPS des collèges (2016-2021)

Historique :
  - PARTIE 1 (lecture S3 + nettoyage) : commit 4, jeudi 26 novembre 2026, Membre A
  - PARTIE 2 (agrégation + Parquet dans processed/) : commit 6, samedi 28 novembre 2026, Membre B

Entrée  : s3://<S3_BUCKET>/bigdata_project/raw/fr-en-ips_colleges.csv
Sorties : s3://<S3_BUCKET>/bigdata_project/processed/
            ips_colleges_clean.parquet     (une ligne par collège et par année, nettoyée)
            ips_academie_secteur.parquet   (IPS moyen par année, académie et secteur)
            ecart_public_prive.parquet     (dataset final : écart public / privé par académie)
            rapport_qualite.json           (nombre de lignes concernées par chaque problème)

Utilisation (terminal du service VSCode-python Onyxia) :
  export S3_BUCKET=user-<identifiant>     (ex. user-t-ince-ece)
  pip install -r src/requirements.txt
  python src/02_pipeline.py

Mode test sans S3 (facultatif) : export LOCAL_DIR=/tmp/test_local
  (le script lit alors LOCAL_DIR/raw/... et écrit dans LOCAL_DIR/processed/)
"""

import io
import json
import os
import re
import unicodedata
from datetime import datetime

import pandas as pd

# Accès S3 commun à tous les scripts (clés lues dans ~/.aws/credentials ou l'environnement)
from s3_utils import afficher_contexte, ouvrir_stockage

# Bornes au-delà desquelles un IPS est considéré comme aberrant (choix documenté dans le rapport).
IPS_MIN, IPS_MAX = 30, 200
FICHIER_BRUT = "raw/fr-en-ips_colleges.csv"


def normaliser_nom(nom):
    """'Rentrée scolaire' -> 'rentree_scolaire' (minuscules, sans accents, sans espaces)."""
    nom = unicodedata.normalize("NFKD", str(nom)).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", nom.lower()).strip("_")


def trouver_colonne(colonnes, motif, obligatoire=True, exclure=None):
    """Cherche la première colonne dont le nom normalisé contient `motif`."""
    for c in colonnes:
        if motif in c and not (exclure and exclure in c):
            return c
    if obligatoire:
        raise SystemExit(f"Colonne contenant '{motif}' introuvable. Colonnes lues : {colonnes}")
    return None


# ============================================================================
# PARTIE 1 : lecture depuis S3 et nettoyage (commit 4)
# ============================================================================
def lire_brut(fs, base):
    """Lit le CSV brut depuis le stockage, en détectant le séparateur et l'encodage."""
    with fs.open(f"{base}/{FICHIER_BRUT}", "rb") as f:
        contenu = f.read()
    try:
        texte = contenu.decode("utf-8-sig")
    except UnicodeDecodeError:
        texte = contenu.decode("latin-1")
    premiere_ligne = texte.split("\n", 1)[0]
    sep = ";" if premiere_ligne.count(";") > premiere_ligne.count(",") else ","
    # Tout est lu en texte : on convertit les types nous-mêmes, pour compter les erreurs.
    df = pd.read_csv(io.StringIO(texte), sep=sep, dtype=str, keep_default_na=False)
    print(f"Lecture : {len(df)} lignes, {df.shape[1]} colonnes, séparateur '{sep}'")
    return df


def nettoyer(df):
    """Nettoie les données et renvoie (df_propre, rapport_qualite)."""
    rapport = {"lignes_brutes": int(len(df))}
    df = df.copy()
    df.columns = [normaliser_nom(c) for c in df.columns]

    # Repérage des colonnes utiles (les noms exacts peuvent varier selon l'export)
    c_rentree = trouver_colonne(df.columns, "rentree")
    c_academie = trouver_colonne(df.columns, "academie")
    c_secteur = trouver_colonne(df.columns, "secteur")
    c_uai = trouver_colonne(df.columns, "uai")
    c_ips = "ips" if "ips" in df.columns else trouver_colonne(df.columns, "ips", exclure="ecart")
    rapport["colonnes"] = list(df.columns)

    # 1. Espaces parasites en début ou fin de texte
    avant = df.copy()
    for c in df.columns:
        df[c] = df[c].str.strip()
    rapport["lignes_avec_espaces_parasites"] = int((avant != df).any(axis=1).sum())

    # 2. Doublons exacts (lignes strictement identiques)
    rapport["doublons_exacts"] = int(df.duplicated().sum())
    df = df.drop_duplicates()

    # 3. Doublons de clé : même collège (UAI) pour la même rentrée, valeurs différentes
    cle = [c_uai, c_rentree]
    rapport["doublons_de_cle_uai_rentree"] = int(df.duplicated(subset=cle).sum())
    df = df.drop_duplicates(subset=cle, keep="first")

    # 4. Valeurs manquantes
    vide = df.replace({"": None, "NA": None, "nan": None}).isna()
    rapport["valeurs_manquantes_par_colonne"] = {c: int(n) for c, n in vide.sum().items() if n > 0}

    # 5. Types : IPS en nombre (virgule décimale acceptée), année extraite de la rentrée
    ips_texte = df[c_ips].str.replace(",", ".", regex=False)
    rapport["ips_avec_virgule_decimale"] = int(df[c_ips].str.contains(",", regex=False).sum())
    df["ips"] = pd.to_numeric(ips_texte, errors="coerce")
    rapport["ips_manquant_ou_non_numerique"] = int(df["ips"].isna().sum())
    df["annee"] = pd.to_numeric(df[c_rentree].str.extract(r"(\d{4})")[0], errors="coerce")
    rapport["rentree_illisible"] = int(df["annee"].isna().sum())

    # 6. Secteur harmonisé : PUBLIC / PRIVE
    s = df[c_secteur].map(lambda v: normaliser_nom(v))
    rapport["libelles_secteur_bruts"] = df[c_secteur].value_counts().to_dict()
    df["secteur"] = s.map(lambda v: "PUBLIC" if v.startswith("pub") else ("PRIVE" if v.startswith("priv") else "AUTRE"))
    rapport["secteur_non_reconnu"] = int((df["secteur"] == "AUTRE").sum())

    # 7. Valeurs aberrantes
    aberrant = df["ips"].notna() & ((df["ips"] < IPS_MIN) | (df["ips"] > IPS_MAX))
    rapport[f"ips_hors_bornes_{IPS_MIN}_{IPS_MAX}"] = int(aberrant.sum())

    # Filtre final : on garde les lignes exploitables
    garde = df["ips"].notna() & df["annee"].notna() & (df["secteur"] != "AUTRE") & ~aberrant
    df = df[garde].copy()
    df["annee"] = df["annee"].astype(int)
    df = df.rename(columns={c_uai: "uai", c_academie: "academie"})
    df["academie"] = df["academie"].str.upper()
    rapport["lignes_finales"] = int(len(df))
    rapport["lignes_supprimees_total"] = rapport["lignes_brutes"] - rapport["lignes_finales"]
    return df, rapport
# === FIN PARTIE 1 ===


def main():
    debut = datetime.now()
    fs, base = ouvrir_stockage()
    afficher_contexte(base)

    # PARTIE 1
    brut = lire_brut(fs, base)
    propre, rapport = nettoyer(brut)
    print("\n=== Rapport qualité ===")
    print(json.dumps(rapport, ensure_ascii=False, indent=2))

    # La PARTIE 2 (agrégation + écriture Parquet) sera ajoutée au commit 6.

    print(f"\nPipeline terminé en {(datetime.now() - debut).total_seconds():.2f} s "
          f"({datetime.now():%Y-%m-%d %H:%M:%S})")


if __name__ == "__main__":
    main()
