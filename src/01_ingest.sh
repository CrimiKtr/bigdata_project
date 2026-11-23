#!/usr/bin/env bash
# =============================================================================
# src/01_ingest.sh : téléchargement des données brutes et envoi sur le S3 Onyxia
# Ajouté au commit 3 (mardi 24 novembre 2026, Membre A)
#
# Ce que fait le script :
#   1. vérifie l'accès au bucket (clés S3 valides) ;
#   2. télécharge le CSV "Indices de position sociale des collèges (2016-2021)"
#      depuis data.gouv.fr (licence Ouverte / Etalab 2.0) ;
#   3. affiche sa taille (doit être entre 1 et 5 Mo) et son empreinte SHA-256 ;
#   4. l'envoie dans s3://<S3_BUCKET>/bigdata_project/raw/ (via src/s3_utils.py) ;
#   5. liste le dossier raw/ : c'est l'écran à capturer (01_bucket_raw.png).
#
# Utilisation (terminal du service VSCode Onyxia, depuis le dossier du dépôt) :
#   export TZ=Europe/Paris
#   export S3_BUCKET=user-<identifiant>        # ex. user-t-ince-ece
#   bash src/01_ingest.sh
# =============================================================================
set -euo pipefail   # arrête le script à la première erreur

# --- 0. Paramètres -----------------------------------------------------------
: "${S3_BUCKET:?Définissez d abord votre bucket : export S3_BUCKET=user-<identifiant>}"
URL="https://www.data.gouv.fr/api/1/datasets/r/b63bd365-c589-48e4-b7d8-9e4f5db133c5"
FICHIER="fr-en-ips_colleges.csv"
DOSSIER_LOCAL="/tmp/bigdata_project_raw"
S3U="python $(dirname "$0")/s3_utils.py"      # petit outil S3 du projet

# --- 1. Contexte et accès S3 -------------------------------------------------
echo "=== Ingestion IPS collèges ==="
${S3U} check                                   # affiche date, namespace, bucket ; s'arrête si les clés ont expiré

# --- 2. Téléchargement -------------------------------------------------------
mkdir -p "${DOSSIER_LOCAL}"
echo "Téléchargement depuis ${URL} ..."
curl -L --fail --silent --show-error -o "${DOSSIER_LOCAL}/${FICHIER}" "${URL}"

# --- 3. Contrôles : taille et empreinte --------------------------------------
TAILLE=$(stat -c%s "${DOSSIER_LOCAL}/${FICHIER}")
echo "Taille : ${TAILLE} octets ($(du -h "${DOSSIER_LOCAL}/${FICHIER}" | cut -f1))"
if [ "${TAILLE}" -lt 1000000 ] || [ "${TAILLE}" -gt 5000000 ]; then
  echo "ATTENTION : la taille n'est pas comprise entre 1 et 5 Mo !" >&2
fi
echo "Nombre de lignes (en-tête compris) : $(wc -l < "${DOSSIER_LOCAL}/${FICHIER}")"
echo "Première ligne (en-tête) :"
head -n 1 "${DOSSIER_LOCAL}/${FICHIER}"

# Petit fichier de traçabilité : date de téléchargement, URL, taille, empreinte
{
  echo "date_telechargement=$(date '+%Y-%m-%d %H:%M:%S %Z')"
  echo "url=${URL}"
  echo "taille_octets=${TAILLE}"
  echo "sha256=$(sha256sum "${DOSSIER_LOCAL}/${FICHIER}" | cut -d' ' -f1)"
} > "${DOSSIER_LOCAL}/manifest.txt"
cat "${DOSSIER_LOCAL}/manifest.txt"

# --- 4. Envoi sur S3 ---------------------------------------------------------
${S3U} put "${DOSSIER_LOCAL}/${FICHIER}" "raw/${FICHIER}"
${S3U} put "${DOSSIER_LOCAL}/manifest.txt" "raw/manifest.txt"

# --- 5. Preuve : contenu du dossier raw/ -------------------------------------
echo "=== Contenu de raw/ ==="
${S3U} ls raw/
echo "Ingestion terminée : $(date '+%Y-%m-%d %H:%M:%S %Z')"
