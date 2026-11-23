"""Connecteur API BOAMP : Pull paginé, incrémental par mois, avec backoff."""
import logging
import os
import random
import time
from datetime import date

import requests
from dotenv import load_dotenv

from connectors.base import Connector

BASE_URL = "https://boamp-datadila.opendatasoft.com/api/explore/v2.1/catalog/datasets/boamp/records"
DESCRIPTEURS_IT = [
    "Informatique (prestations de services)",
    "Logiciel",
    "Informatique (matériel)",
    "Informatique (maintenance serveurs et réseaux)",
    "Télécommunications",
]
PAGE_SIZE = 100                                  # maximum accepté par l'API
STATUTS_A_REESSAYER = {429, 500, 502, 503, 504}  # erreurs transitoires
NB_TENTATIVES = 5


def mois_suivant(mois):
    annee, m = map(int, mois.split("-"))
    return f"{annee + 1}-01" if m == 12 else f"{annee}-{m + 1:02d}"


def liste_mois(debut, fin):
    """Tous les mois de debut à fin inclus, au format AAAA-MM."""
    mois = [debut]
    while mois[-1] < fin:
        mois.append(mois_suivant(mois[-1]))
    return mois


def construire_where(mois):
    """Filtre de l'API : annonces IT publiées pendant le mois."""
    descripteurs = " OR ".join(f'descripteur_libelle="{d}"' for d in DESCRIPTEURS_IT)
    return (
        f"dateparution >= date'{mois}-01' "
        f"AND dateparution < date'{mois_suivant(mois)}-01' "
        f"AND ({descripteurs})"
    )


def get_avec_backoff(params):
    """GET avec backoff exponentiel + jitter (cours : rate limiting et backoff)."""
    for tentative in range(NB_TENTATIVES):
        try:
            reponse = requests.get(BASE_URL, params=params, timeout=30)
        except (requests.Timeout, requests.ConnectionError) as erreur:
            attente = 2 ** tentative + random.uniform(0, 1)
            logging.warning("Erreur réseau (%s), nouvel essai dans %.1f s", erreur, attente)
            time.sleep(attente)
            continue
        if reponse.status_code in STATUTS_A_REESSAYER:
            attente = int(reponse.headers.get("Retry-After", 2 ** tentative)) + random.uniform(0, 1)
            logging.warning("HTTP %d, nouvel essai dans %.1f s", reponse.status_code, attente)
            time.sleep(attente)
            continue
        reponse.raise_for_status()   # 4xx : erreur définitive, pas de retry
        return reponse.json()
    raise RuntimeError(f"Abandon après {NB_TENTATIVES} tentatives")


class BoampApiConnector(Connector):
    source = "api-boamp"
    nom_etat = "mois_api_publies"

    def __init__(self, mois_debut, bootstrap=None):
        super().__init__(bootstrap)
        self.mois_debut = mois_debut

    def extract(self):
        mois_en_cours = date.today().strftime("%Y-%m")
        for mois in liste_mois(self.mois_debut, mois_en_cours):
            if mois in self.termines:
                continue
            offset = 0
            while True:   # pagination offset/limit
                page = get_avec_backoff({
                    "where": construire_where(mois),
                    "limit": PAGE_SIZE,
                    "offset": offset,
                    "order_by": "idweb",   # tri stable pour paginer sans trou
                })
                yield from page["results"]
                offset += PAGE_SIZE
                if offset >= page["total_count"]:
                    break
            logging.info("%s : %d annonces", mois, page["total_count"])
            if mois != mois_en_cours:   # le mois en cours n'est jamais terminé
                self.termines.add(mois)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    BoampApiConnector(os.environ.get("BOAMP_MOIS_DEBUT", "2026-11")).run()
