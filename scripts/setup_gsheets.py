"""
setup_gsheets.py
----------------
Creates Google Sheets tabs for all niches. Skips tabs that already exist.
Run once before launching the scraper.

Usage:
    python scripts/setup_gsheets.py
"""

import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials

load_dotenv(Path(__file__).parent.parent / ".env")

SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_MASTER_SPREADSHEET_ID")
CREDENTIALS_FILE = Path(__file__).parent.parent / "config" / "google_credentials.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

NICHE_SHEET_MAP = {
    "electricien": "Electricien",
    "plombier": "Plombier",
    "serrurier": "Serrurier",
    "cgp": "CGP",
    "iad": "IAD",
    "agent_immo": "Agent Immo",
}

NICHES = [
    "electricien",
    "plombier",
    "serrurier",
    "garage_auto",
    "clinique_dentaire",
    "architecte_interieur",
    "architecte",
    "hotel",
    "cgp",
    "comptable",
    "restaurant",
    "iad",
    "agent_immo",
]

HEADERS = [
    "Company",
    "City name",
    "Address",
    "Phone",
    "Mail",
    "Status",
    "Commentaire",
    "Nombre d'avis",
    "Note moyenne",
    "Département",
    "Date d'ajout",
    "Date de mise à jour",
]


def main():
    if not SPREADSHEET_ID:
        sys.exit("GOOGLE_SHEETS_MASTER_SPREADSHEET_ID manquant dans .env")

    if not CREDENTIALS_FILE.exists():
        sys.exit(f"Credentials introuvable: {CREDENTIALS_FILE}")

    with open(CREDENTIALS_FILE, "r", encoding="utf-8") as f:
        creds_dict = json.load(f)

    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SPREADSHEET_ID)

    print(f"Spreadsheet: {spreadsheet.title}")
    print(f"Création des onglets pour {len(NICHES)} niches...\n")

    existing_titles = {ws.title for ws in spreadsheet.worksheets()}

    created = 0
    skipped = 0

    for niche in NICHES:
        title = NICHE_SHEET_MAP.get(niche, niche)
        if title in existing_titles:
            print(f"  — {title:<25} (déjà existant, skip)")
            skipped += 1
            continue
        try:
            ws = spreadsheet.add_worksheet(title=title, rows=5000, cols=len(HEADERS))
            ws.insert_row(HEADERS, 1)
            ws.format(f"A1:{chr(ord('A') + len(HEADERS) - 1)}1", {"textFormat": {"bold": True}})
            print(f"  ✓ {title:<25} créé")
            created += 1
        except Exception as e:
            print(f"  ✗ {title:<25} erreur: {e}")

    print(f"\n{created} créés, {skipped} skippés.")


if __name__ == "__main__":
    main()
