import os
import json
import logging
from typing import Dict, Any, Optional
import gspread
from gspread.exceptions import WorksheetNotFound
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

class GoogleSheetsService:
    def __init__(self, main_spreadsheet_id: Optional[str] = None):
        self.scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        self.client = None
        self.credentials = None
        self.main_spreadsheet = None
        self.main_spreadsheet_id = (
            main_spreadsheet_id or os.getenv("GOOGLE_SHEETS_MASTER_SPREADSHEET_ID")
        )
        
        # Cache pour stocker les téléphones existants par niche
        self._existing_phones_cache: Dict[str, set] = {}
        # Cache pour les agents IMMO
        self._existing_immo_phones_cache: set = set()

        logger.info("GoogleSheetsService VERSION ONGLETS")

        try:
            credentials_file = "config/google_credentials.json"

            if not os.path.exists(credentials_file):
                logger.error(f"Fichier de credentials introuvable: {credentials_file}")
                return

            with open(credentials_file, "r", encoding="utf-8") as f:
                creds_dict = json.load(f)

            self.credentials = Credentials.from_service_account_info(
                creds_dict,
                scopes=self.scopes
            )
            self.client = gspread.authorize(self.credentials)
            logger.info("Service Google Sheets initialisé avec succès.")

            if not self.main_spreadsheet_id:
                logger.error("GOOGLE_SHEETS_MASTER_SPREADSHEET_ID absent.")
                self.client = None
                return

            logger.info(f"MASTER SPREADSHEET ID: {self.main_spreadsheet_id}")
            self.main_spreadsheet = self.client.open_by_key(self.main_spreadsheet_id)
            logger.info(f"Spreadsheet principal ouvert: {self.main_spreadsheet.title}")

        except json.JSONDecodeError as e:
            logger.error(f"Erreur JSON dans le fichier de credentials: {e}", exc_info=True)
            self.client = None
        except Exception as e:
            logger.error(f"Erreur lors de l'initialisation du service Google Sheets: {e}", exc_info=True)
            self.client = None

    def _sanitize_worksheet_title(self, niche_name: str) -> str:
        """Nettoie et valide le nom de l'onglet."""
        title = (niche_name or "").strip()
        if not title:
            raise ValueError("Le nom de niche est vide.")
        return title[:100]

    def ensure_sheet_exists(self, niche_name: str) -> bool:
        """
        Vérifie si un onglet existe pour cette niche dans le spreadsheet principal.
        Le crée si absent. Retourne True si l'onglet est disponible, False sinon.
        """
        if not self.client or not self.main_spreadsheet:
            logger.error("Client Google Sheets ou spreadsheet principal non initialisé.")
            return False

        try:
            title = self._sanitize_worksheet_title(niche_name)

            try:
                ws = self.main_spreadsheet.worksheet(title)
                logger.info(f"Onglet trouvé pour la niche {title}")

                headers = ws.row_values(1)
                if not headers:
                    self._create_headers_and_validation(ws)

                return True

            except WorksheetNotFound:
                logger.info(f"Création d'un nouvel onglet pour la niche {title}...")
                ws = self.main_spreadsheet.add_worksheet(title=title, rows=1000, cols=10)
                self._create_headers_and_validation(ws)
                logger.info(f"Onglet créé avec succès pour {title}")
                return True

        except Exception as e:
            logger.error(f"Erreur lors de la vérification/création de l'onglet pour {niche_name}: {e}", exc_info=True)
            return False

    def get_existing_phones(self, niche_name: str) -> set:
        """
        Récupère tous les numéros de téléphone existants dans l'onglet pour éviter les doublons.
        Utilise un cache en mémoire pour ne faire la requête qu'une seule fois par session.
        """
        if not self.client or not self.main_spreadsheet:
            return set()

        title = self._sanitize_worksheet_title(niche_name)
        
        # Retourner depuis le cache si déjà chargé
        if title in self._existing_phones_cache:
            return self._existing_phones_cache[title]

        existing_phones = set()
        try:
            # Vérifier si l'onglet existe d'abord
            try:
                sheet = self.main_spreadsheet.worksheet(title)
            except WorksheetNotFound:
                # Si l'onglet n'existe pas, il n'y a pas de doublons
                self._existing_phones_cache[title] = existing_phones
                return existing_phones

            # Récupérer toutes les valeurs de la colonne D (Phone)
            # row_values et col_values sont indexés à partir de 1
            phones = sheet.col_values(4)
            
            # Ignorer le header (première ligne)
            for phone in phones[1:]:
                if phone:
                    existing_phones.add(phone.strip())
                    
            logger.info(f"Chargement de {len(existing_phones)} numéros existants pour la niche {title}")
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des numéros existants pour {title}: {e}", exc_info=True)
            
        # Sauvegarder dans le cache
        self._existing_phones_cache[title] = existing_phones
        return existing_phones

    def send_to_gsheets(
        self,
        niche_name: str,
        lead_data: Dict[str, Any],
        email: str,
        city_name: str,
        department: str = "",
        sent_emails_set: set = None
    ) -> bool:
        """Envoyer un lead vers Google Sheets."""
        if not self.client or not self.main_spreadsheet or not niche_name:
            return False

        phone = lead_data.get("phone")

        # téléphone obligatoire
        if not phone:
            return False
            
        # Formatage du numéro avec espaces (ex: "06 28 11 26 31")
        phone_clean = self._format_phone_display(str(phone).strip())
        phone_for_dedup = phone_clean.replace(" ", "")

        # Vérification globale des doublons dans le Google Sheet
        existing_phones = self.get_existing_phones(niche_name)
        if phone_for_dedup in existing_phones:
            logger.debug(f"GSheets: Lead avec téléphone {phone_for_dedup} déjà présent pour {niche_name}")
            return False

        if sent_emails_set is not None:
            # On utilise le téléphone comme identifiant unique pour les doublons en session
            lead_id = phone_for_dedup
            if lead_id in sent_emails_set:
                return False

        # Gérer les avis: vide si None
        reviews_count = lead_data.get("numberOfRate")
        average_rating = lead_data.get("averageRate")

        row_data = {
            "company": lead_data.get("name", ""),
            "city": city_name,
            "address": lead_data.get("address", ""),
            "phone": phone_clean,
            "email": email or "",
            "status": "Nouveau",
            "comment": "",
            "reviews_count": reviews_count if reviews_count is not None else "",
            "average_rating": average_rating if average_rating is not None else "",
            "department": department
        }

        success = self.add_lead_to_sheet(niche_name, row_data)
        if success:
            # Ajouter au cache local pour éviter les futurs doublons dans la même session
            title = self._sanitize_worksheet_title(niche_name)
            if title in self._existing_phones_cache:
                self._existing_phones_cache[title].add(phone_clean)
                
            if sent_emails_set is not None:
                sent_emails_set.add(phone_for_dedup)
            logger.info(f"GSheets: Lead sans site ajouté à l'onglet {niche_name}")

        return success

    def add_lead_to_sheet(self, niche_name: str, lead_data: Dict[str, Any]) -> bool:
        """Ajoute un lead à l'onglet spécifique de la niche dans le spreadsheet principal."""
        if not self.client or not self.main_spreadsheet:
            logger.error("Client Google Sheets ou spreadsheet principal non initialisé.")
            return False

        try:
            if not self.ensure_sheet_exists(niche_name):
                return False

            title = self._sanitize_worksheet_title(niche_name)
            sheet = self.main_spreadsheet.worksheet(title)

            headers = sheet.row_values(1)
            if not headers:
                self._create_headers_and_validation(sheet)

            # Forcer le format texte pour le téléphone en ajoutant un apostrophe
            phone_value = lead_data.get("phone", "")
            if phone_value:
                phone_value = f"'{phone_value}"
            
            row_data = [
                lead_data.get("company", ""),
                lead_data.get("city", ""),
                lead_data.get("address", ""),
                phone_value,
                lead_data.get("email", ""),
                lead_data.get("status", "Nouveau"),
                lead_data.get("comment", ""),
                lead_data.get("reviews_count", ""),
                lead_data.get("average_rating", ""),
                lead_data.get("department", "")
            ]

            sheet.append_row(row_data, value_input_option="USER_ENTERED")
            return True

        except Exception as e:
            logger.error(f"Erreur lors de l'ajout au sheet {niche_name}: {e}", exc_info=True)
            return False
            
    def _format_phone_display(self, phone: str) -> str:
        """Formate un numéro de téléphone pour l'affichage (ex: '06 28 11 26 31')."""
        if not phone:
            return ""
        # Enlever tous les espaces et caractères spéciaux
        digits = ''.join(c for c in str(phone) if c.isdigit())
        if len(digits) == 10:
            # Format français: XX XX XX XX XX
            return ' '.join([digits[i:i+2] for i in range(0, 10, 2)])
        elif len(digits) > 10:
            # Si plus de 10 chiffres, prendre les 10 derniers (ex: +33 6 28 11 26 31)
            digits = digits[-10:]
            return ' '.join([digits[i:i+2] for i in range(0, 10, 2)])
        return phone

    def _create_headers_and_validation(self, sheet):
        """Crée les en-têtes (la validation des données a été retirée car gspread ne la supporte pas directement sur Worksheet)."""
        headers = [
            "Company",
            "City name",
            "Address",
            "Phone",
            "Mail",
            "Status",
            "Commentaire",
            "Nombre d'avis",
            "Note moyenne",
            "Département"
        ]

        sheet.insert_row(headers, 1)
        sheet.format("A1:J1", {"textFormat": {"bold": True}})
        logger.info("En-têtes créés avec succès.")
