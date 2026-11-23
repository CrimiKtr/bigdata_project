"""
src/s3_utils.py : accès au stockage S3 d'Onyxia, partagé par tous les scripts du projet
Ajouté au commit 3 (mardi 24 novembre 2026, Membre A)

D'où viennent les clés S3 ?
  1. du fichier ~/.aws/credentials s'il existe. C'est le fichier écrit par le script
     « AWS CLI / shared profile » copié depuis Onyxia (Stockage de données → ⚙ Paramètres
     du profil). On le recharge au début de chaque séance, car les clés expirent ;
  2. sinon, des variables d'environnement AWS_* fournies par Onyxia au démarrage du service.
  Les clés ne sont JAMAIS écrites dans le dépôt Git.

Petites commandes utiles (depuis le dossier du dépôt) :
  python src/s3_utils.py check            # vérifie l'accès au bucket
  python src/s3_utils.py ls raw/          # liste un dossier du projet (preuve pour les captures)
  python src/s3_utils.py ls               # liste tout le dossier bigdata_project/
  python src/s3_utils.py cat raw/manifest.txt
  python src/s3_utils.py put <fichier_local> raw/<nom>

Mode test sans S3 (facultatif) : export LOCAL_DIR=/tmp/test_local
"""

import configparser
import os
import socket
import sys
from datetime import datetime

PROJET = "bigdata_project"
FICHIER_CLES = os.path.expanduser("~/.aws/credentials")


def _cles():
    """Renvoie (key, secret, token, origine)."""
    if os.path.exists(FICHIER_CLES):
        cfg = configparser.ConfigParser()
        cfg.read(FICHIER_CLES)
        if cfg.has_section("default"):
            s = cfg["default"]
            date = datetime.fromtimestamp(os.path.getmtime(FICHIER_CLES)).strftime("%d/%m/%Y %H:%M")
            return (s.get("aws_access_key_id"), s.get("aws_secret_access_key"),
                    s.get("aws_session_token"), f"fichier ~/.aws/credentials (rechargé le {date})")
    return (os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY"),
            os.environ.get("AWS_SESSION_TOKEN"), "variables d'environnement du service")


def ouvrir_stockage(verbeux=True):
    """Renvoie (fs, base) : un système de fichiers et le chemin du projet dans le bucket."""
    if os.environ.get("LOCAL_DIR"):
        import fsspec
        return fsspec.filesystem("file", auto_mkdir=True), os.environ["LOCAL_DIR"].rstrip("/")
    import s3fs
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise SystemExit("Définissez votre bucket : export S3_BUCKET=user-<identifiant>  (ex. user-t-ince-ece)")
    endpoint = os.environ.get("AWS_S3_ENDPOINT", "").replace("https://", "").replace("http://", "")
    if not endpoint:
        raise SystemExit("Variable AWS_S3_ENDPOINT absente : lancez le script depuis un service Onyxia.")
    key, secret, token, origine = _cles()
    if verbeux:
        print("Clés S3           :", origine)
    fs = s3fs.S3FileSystem(
        key=key, secret=secret, token=token,
        client_kwargs={"endpoint_url": "https://" + endpoint, "region_name": "us-east-1"},
        config_kwargs={"s3": {"addressing_style": "path"}},  # adressage « path » comme Onyxia
    )
    return fs, f"{bucket}/{PROJET}"


def afficher_contexte(base):
    """Affiche date, heure, namespace et chemin : à garder visible sur les captures."""
    ns_file = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    namespace = open(ns_file).read().strip() if os.path.exists(ns_file) else "(hors Onyxia)"
    print("Date et heure     :", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("Namespace Onyxia  :", namespace)
    print("Machine           :", socket.gethostname())
    print("Chemin du projet  :", ("s3://" if not os.environ.get("LOCAL_DIR") else "") + base)


def lister(fs, base, sous_dossier=""):
    """Affiche les fichiers d'un dossier du projet avec taille et date."""
    chemin = f"{base}/{sous_dossier}".rstrip("/")
    if not fs.exists(chemin):
        print(f"(dossier vide ou inexistant : {chemin})")
        return
    for f in sorted(fs.find(chemin, detail=True).values(), key=lambda d: d["name"]):
        date = f.get("LastModified") or f.get("mtime") or ""
        if hasattr(date, "strftime"):
            date = date.astimezone().strftime("%Y-%m-%d %H:%M")  # heure locale
        elif isinstance(date, (int, float)):
            date = datetime.fromtimestamp(date).strftime("%Y-%m-%d %H:%M")
        print(f"{f.get('size', 0):>12} octets  {date:<16}  {f['name']}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in {"check", "ls", "cat", "put"}:
        print(__doc__)
        return
    commande = sys.argv[1]
    fs, base = ouvrir_stockage()
    afficher_contexte(base)
    if commande == "check":
        bucket = base.split("/")[0]
        try:
            fs.ls(bucket)
            print("ACCÈS S3 OK : le bucket", bucket, "est lisible.")
        except Exception as e:  # message simple pour un débutant
            print("ACCÈS S3 REFUSÉ :", type(e).__name__, e)
            print("→ Rechargez vos clés : Onyxia → Stockage de données → ⚙ → « Renouveler les jetons »,")
            print("  puis copiez le script « AWS CLI / shared profile » et collez-le dans ce terminal.")
            sys.exit(1)
    elif commande == "ls":
        lister(fs, base, sys.argv[2] if len(sys.argv) > 2 else "")
    elif commande == "cat":
        with fs.open(f"{base}/{sys.argv[2]}", "r") as f:
            print(f.read())
    elif commande == "put":
        local, distant = sys.argv[2], sys.argv[3]
        fs.put(local, f"{base}/{distant}")
        print("Envoyé :", local, "→", f"s3://{base}/{distant}")


if __name__ == "__main__":
    main()
