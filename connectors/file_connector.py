"""Connecteur fichiers : lit les JSON Lines historiques de data/input/."""
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from connectors.base import Connector

INPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "input"


class FileConnector(Connector):
    source = "boamp-fichiers"
    nom_etat = "fichiers_publies"

    def __init__(self, motif="boamp_*.jsonl", dossier=INPUT_DIR, bootstrap=None):
        super().__init__(bootstrap)
        self.motif = motif
        self.dossier = Path(dossier)

    def extract(self):
        for chemin in sorted(self.dossier.glob(self.motif)):
            if chemin.name in self.termines:      # déjà publié : on saute
                continue
            with open(chemin, encoding="utf-8") as f:
                for numero, ligne in enumerate(f, start=1):
                    try:
                        yield json.loads(ligne)
                    except json.JSONDecodeError:
                        logging.warning("Ligne illisible : %s ligne %d", chemin.name, numero)
            self.termines.add(chemin.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    motif = sys.argv[1] if len(sys.argv) > 1 else "boamp_*.jsonl"
    FileConnector(motif).run()
