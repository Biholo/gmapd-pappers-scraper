import os
import json
import logging
from datetime import date
from typing import Dict, Any, Optional
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

NICHE_SHEET_MAP: Dict[str, str] = {
    "electricien": "Electricien",
    "plombier": "Plombier",
    "serrurier": "Serrurier",
    "cgp": "CGP",
    "iad": "IAD",
    "agent_immo": "Agent Immo",
}


class GoogleSheetsService:
    def __init__(self, main_spreadsheet_id: Optional[str] = None):
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        self.client = None
        self.credentials = None
        self.main_spreadsheet = None
        self.main_spreadsheet_id = (
            main_spreadsheet_id or os.getenv("GOOGLE_SHEETS_MASTER_SPREADSHEET_ID")
        )
        self._existing_phones_cache: Dict[str, set] = {}

        try:
            credentials_file = "config/google_credentials.json"
            if not os.path.exists(credentials_file):
                logger.error(f"Fichier de credentials introuvable: {credentials_file}")
                return

            with open(credentials_file, "r", encoding="utf-8") as f:
                creds_dict = json.load(f)

            self.credentials = Credentials.from_service_account_info(
                creds_dict, scopes=self.scopes
            )
            self.client = gspread.authorize(self.credentials)
            logger.info("Service Google Sheets initialisé.")

            if not self.main_spreadsheet_id:
                logger.error("GOOGLE_SHEETS_MASTER_SPREADSHEET_ID absent.")
                self.client = None
                return

            self.main_spreadsheet = self.client.open_by_key(self.main_spreadsheet_id)
            logger.info(f"Spreadsheet principal ouvert: {self.main_spreadsheet.title}")

        except json.JSONDecodeError as e:
            logger.error(f"Erreur JSON credentials: {e}", exc_info=True)
            self.client = None
        except Exception as e:
            logger.error(f"Erreur initialisation Google Sheets: {e}", exc_info=True)
            self.client = None

    def _sanitize_worksheet_title(self, niche_name: str) -> str:
        key = (niche_name or "").strip()
        if not key:
            raise ValueError("Le nom de niche est vide.")
        return NICHE_SHEET_MAP.get(key, key)[:100]

    def _get_worksheet(self, title: str):
        """Force-refresh internal gspread cache then return worksheet, or None."""
        try:
            self.main_spreadsheet.fetch_sheet_metadata()
        except Exception:
            pass
        try:
            return self.main_spreadsheet.worksheet(title)
        except WorksheetNotFound:
            logger.warning(f"Onglet '{title}' introuvable. Lancer scripts/setup_gsheets.py.")
            return None

    def get_existing_phones(self, niche_name: str) -> set:
        """Load existing phone numbers for deduplication (cached per session)."""
        if not self.client or not self.main_spreadsheet:
            return set()

        title = self._sanitize_worksheet_title(niche_name)
        if title in self._existing_phones_cache:
            return self._existing_phones_cache[title]

        existing_phones = set()
        try:
            sheet = self._get_worksheet(title)
            if sheet is not None:
                phones = sheet.col_values(4)
                for phone in phones[1:]:
                    if phone:
                        existing_phones.add(phone.strip())
                logger.info(f"{len(existing_phones)} numéros existants chargés pour {title}")
        except Exception as e:
            logger.error(f"Erreur chargement numéros {title}: {e}", exc_info=True)

        self._existing_phones_cache[title] = existing_phones
        return existing_phones

    def send_to_gsheets(
        self,
        niche_name: str,
        lead_data: Dict[str, Any],
        email: str,
        city_name: str,
        department: str = "",
        sent_emails_set: set = None,
    ) -> bool:
        if not self.client or not self.main_spreadsheet or not niche_name:
            return False

        phone = lead_data.get("phone")
        if not phone:
            return False

        phone_clean = self._format_phone_display(str(phone).strip())
        phone_for_dedup = phone_clean.replace(" ", "")

        existing_phones = self.get_existing_phones(niche_name)
        if phone_for_dedup in existing_phones:
            logger.debug(f"GSheets: {phone_for_dedup} déjà présent pour {niche_name}")
            return False

        if sent_emails_set is not None and phone_for_dedup in sent_emails_set:
            return False

        today = date.today().strftime("%d/%m/%Y")
        row_data = {
            "company": lead_data.get("name", ""),
            "city": city_name,
            "address": lead_data.get("address", ""),
            "phone": phone_clean,
            "email": email or "",
            "status": "Nouveau",
            "comment": "",
            "reviews_count": lead_data.get("numberOfRate") if lead_data.get("numberOfRate") is not None else "",
            "average_rating": lead_data.get("averageRate") if lead_data.get("averageRate") is not None else "",
            "department": department,
            "date_ajout": today,
            "date_maj": today,
        }

        success = self.add_lead_to_sheet(niche_name, row_data)
        if success:
            title = self._sanitize_worksheet_title(niche_name)
            if title in self._existing_phones_cache:
                self._existing_phones_cache[title].add(phone_clean)
            if sent_emails_set is not None:
                sent_emails_set.add(phone_for_dedup)
            logger.info(f"GSheets: Lead ajouté à l'onglet {niche_name}")

        return success

    def add_lead_to_sheet(self, niche_name: str, lead_data: Dict[str, Any]) -> bool:
        if not self.client or not self.main_spreadsheet:
            return False

        try:
            title = self._sanitize_worksheet_title(niche_name)
            sheet = self._get_worksheet(title)
            if sheet is None:
                return False

            phone_value = lead_data.get("phone", "")
            if phone_value:
                phone_value = f"'{phone_value}"

            today = date.today().strftime("%d/%m/%Y")
            row = [
                lead_data.get("company", ""),
                lead_data.get("city", ""),
                lead_data.get("address", ""),
                phone_value,
                lead_data.get("email", ""),
                lead_data.get("status", "Nouveau"),
                lead_data.get("comment", ""),
                lead_data.get("reviews_count", ""),
                lead_data.get("average_rating", ""),
                lead_data.get("department", ""),
                lead_data.get("date_ajout", today),
                lead_data.get("date_maj", today),
            ]

            sheet.append_row(row, value_input_option="USER_ENTERED")
            return True

        except Exception as e:
            logger.error(f"Erreur ajout au sheet {niche_name}: {e}", exc_info=True)
            return False

    def _format_phone_display(self, phone: str) -> str:
        if not phone:
            return ""
        digits = "".join(c for c in str(phone) if c.isdigit())
        if len(digits) >= 10:
            digits = digits[-10:]
            return " ".join([digits[i : i + 2] for i in range(0, 10, 2)])
        return phone
