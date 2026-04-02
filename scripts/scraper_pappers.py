#!/usr/bin/env python3
"""
🕷️ Scraper amélioré pour les SCI sur Pappers.fr avec enrichissement Yahoo
Version avec extraction détaillée des dirigeants et profils LinkedIn via Yahoo
"""

import asyncio
import csv
import html
import json
import re
import time
from datetime import datetime
from urllib.parse import quote, unquote, urlparse, parse_qs
from playwright.async_api import async_playwright
import os
import sys
import logging
from dotenv import load_dotenv

# Ajouter la racine du projet au path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Charger les variables d'environnement depuis le fichier .env à la racine
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)
else:
    load_dotenv()

from services.getsales_service import GetSalesService
from services.resend_service import ResendService

# Configuration du logger (console)
logger = logging.getLogger("sci_scraper")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO))


class SciScraperEnhanced:
    # Formes juridiques par type de scraping
    FORME_JURIDIQUE_SCI = '6540'
    FORME_JURIDIQUE_COMPANY = '5720,5710,5498,5499'  # SASU, SAS, EURL, SARL

    def __init__(self, headless=True, max_pages=None, scrape_type="sci"):
        """
        Args:
            scrape_type: "sci" pour SCI, "company" pour SASU/SAS/EURL/SARL
        """
        self.headless = headless
        self.max_pages = max_pages  # None = pas de limite
        self.scrape_type = scrape_type  # "sci" ou "company"
        self.csv_file = None
        self.csv_writer = None
        self.total_companies = 0
        self.start_time = None
        # Yahoo enrichment
        self.google_page = None
        self.storage_state_path = "enrichment_yahoo_state.json"
        self.enrichment_queries_count = 0
        # Envoi backend
        self.consecutive_send_failures = 0
        self.stop_requested = False
        # Logger
        self.logger = logger
        # Set pour éviter les doublons
        self.existing_sirens = set()
        # Persistance pagination (fichier different par type)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.config_dir = os.path.join(base_dir, 'config')
        os.makedirs(self.config_dir, exist_ok=True)
        self.config_file_path = os.path.join(self.config_dir, f'scraper_config_{scrape_type}.json' if scrape_type != "sci" else 'scraper_config.json')
        self.resume_page = 1
        self.current_page_num = 1
        # Filtres de recherche (config JSON)
        if scrape_type == "company":
            self.forme_juridique = self.FORME_JURIDIQUE_COMPANY
        else:
            self.forme_juridique = self.FORME_JURIDIQUE_SCI
        self.en_activite = True
        self.date_creation_min = None  # format attendu: JJ-MM-AAAA (ex: 15-08-2025)
        self.date_creation_max = None
        self.scraping_status = 'initialise'  # Status du scraping
        # Tracking par jour pour le rapport
        self.days_stats = []  # [{"date": "27-03-2026", "count": 12}, ...]
        self.current_day_count = 0

    def fix_encoding(self, text):
        """Corriger les problèmes d'encodage"""
        if not text:
            return text

        # Décoder les entités HTML
        fixed = html.unescape(text)

        # Corriger les caractères mal encodés
        fixes = {
            'Ã': 'é', 'Ã¨': 'è', 'Ã ': 'à', 'Ã´': 'ô', 'Ã«': 'ë',
            'Ã¢': 'â', 'Ã¹': 'ù', 'Ã§': 'ç', 'Ã®': 'î', 'Ãª': 'ê',
            'â€¯': ' ', 'â‚¬': '€', 'â€™': "'", 'â€œ': '"', 'â€': '"'
        }

        for wrong, correct in fixes.items():
            fixed = fixed.replace(wrong, correct)

        return fixed.strip()

    def load_resume_page(self) -> int:
        """Charger la page de reprise depuis le fichier de configuration (si présent)."""
        try:
            if os.path.exists(self.config_file_path):
                with open(self.config_file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        page = data.get('resume_page')
                        last_update = data.get('last_update', 'inconnue')
                        last_status = data.get('scraping_status', 'inconnu')
                        last_total = data.get('total_companies_scraped', 0)

                        # Afficher l'historique si pertinent
                        if last_status and last_status != 'initialise':
                            self.logger.info(f"📊 DERNIER ÉTAT SAUVEGARDÉ:")
                            self.logger.info(f"   Status: {last_status}")
                            self.logger.info(f"   Mise à jour: {last_update}")
                            self.logger.info(f"   Total entreprises: {last_total}")

                        if isinstance(page, int) and page >= 1:
                            self.resume_page = page
                            return page
        except Exception:
            pass
        self.resume_page = 1
        return 1

    def save_resume_page(self, next_page: int, status: str = None) -> None:
        """Sauvegarder la prochaine page à traiter dans le fichier de configuration.

        Args:
            next_page: Prochaine page à traiter
            status: État du scraping (en_cours, termine, interrompu, changement_date)
        """
        try:
            data = {}
            if os.path.exists(self.config_file_path):
                try:
                    with open(self.config_file_path, 'r', encoding='utf-8') as f:
                        existing = json.load(f)
                        if isinstance(existing, dict):
                            data.update(existing)
                except Exception:
                    pass

            # Sauvegarder l'état complet
            data['resume_page'] = max(1, int(next_page))
            data['last_update'] = datetime.now().isoformat()
            data['total_companies_scraped'] = self.total_companies

            # Ajouter le status si fourni
            if status:
                data['scraping_status'] = status
                self.scraping_status = status

            # Logging détaillé de l'état
            self.logger.info(f"💾 SAUVEGARDE CONFIG:")
            self.logger.info(f"   📄 Page suivante: {next_page}")
            self.logger.info(f"   📅 Dates: {self.date_creation_min} → {self.date_creation_max}")
            self.logger.info(f"   🏢 Total entreprises: {self.total_companies}")
            if status:
                self.logger.info(f"   ⚡ Status: {status}")

            with open(self.config_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Erreur sauvegarde config: {e}")

    def load_search_filters(self) -> None:
        """Charger les filtres de recherche (dates, activité, forme juridique) depuis la config JSON.

        Note: Pour simplifier, on utilise une date unique (min = max) pour rechercher jour par jour.
        """
        try:
            if os.path.exists(self.config_file_path):
                with open(self.config_file_path, 'r', encoding='utf-8') as f:
                    cfg = json.load(f)
                    if isinstance(cfg, dict):
                        fj = cfg.get('forme_juridique')
                        if isinstance(fj, str) and fj.strip():
                            self.forme_juridique = fj.strip()
                        en_act = cfg.get('en_activite')
                        if isinstance(en_act, bool):
                            self.en_activite = en_act
                        dmin = cfg.get('date_creation_min')
                        if isinstance(dmin, str) and dmin.strip():
                            self.date_creation_min = dmin.strip()
                        dmax = cfg.get('date_creation_max')
                        if isinstance(dmax, str) and dmax.strip():
                            self.date_creation_max = dmax.strip()

                        # Synchroniser min et max pour recherche jour par jour
                        if self.date_creation_min and not self.date_creation_max:
                            self.date_creation_max = self.date_creation_min
                        elif self.date_creation_max and not self.date_creation_min:
                            self.date_creation_min = self.date_creation_max
                        elif self.date_creation_min != self.date_creation_max:
                            # Si différentes, on prend la plus récente (min)
                            self.logger.info(f"⚠️ Dates min/max différentes détectées. Utilisation de: {self.date_creation_min}")
                            self.date_creation_max = self.date_creation_min
        except Exception as e:
            self.logger.error(f"Erreur chargement filtres: {e}")

    def build_search_url(self, page_num: int) -> str:
        """Construire l'URL de recherche Pappers avec les filtres configurés."""
        base = "https://www.pappers.fr/recherche"
        params = []
        if self.forme_juridique:
            params.append(f"forme_juridique={self.forme_juridique}")
        if self.en_activite:
            params.append("en_activite=true")
        if self.date_creation_min:
            params.append(f"date_creation_min={self.date_creation_min}")
        if self.date_creation_max:
            params.append(f"date_creation_max={self.date_creation_max}")
        if page_num and page_num > 1:
            params.append(f"page={page_num}")
        query = '&'.join(params)
        return f"{base}?{query}" if query else base

    # ===== Helpers de date/filtre =====
    @staticmethod
    def _parse_fr_date(date_str: str):
        try:
            return datetime.strptime(date_str, "%d-%m-%Y").date()
        except Exception:
            return None

    @staticmethod
    def _format_fr_date(date_obj) -> str:
        try:
            return date_obj.strftime("%d-%m-%Y")
        except Exception:
            return None

    def shift_date_filters_by_days(self, days: int) -> None:
        """Décaler les dates min/max de 'days' jours (négatif pour revenir en arrière).
        Note: Les dates min et max sont toujours identiques (recherche par jour unique).
        """
        try:
            if self.date_creation_min:
                dmin = self._parse_fr_date(self.date_creation_min)
                if dmin:
                    new_date = dmin.fromordinal(dmin.toordinal() + days)
                    new_date_str = self._format_fr_date(new_date)
                    if new_date_str:
                        # Garder min et max identiques pour recherche par jour
                        self.date_creation_min = new_date_str
                        self.date_creation_max = new_date_str
        except Exception as e:
            self.logger.error(f"Erreur lors du décalage de date: {e}")

    def save_filters_to_config(self) -> None:
        """Persister les filtres actuels (dates, activité, forme) dans le fichier de config.

        Note: Les dates min et max sont toujours identiques (recherche jour par jour).
        """
        try:
            data = {}
            if os.path.exists(self.config_file_path):
                try:
                    with open(self.config_file_path, 'r', encoding='utf-8') as f:
                        existing = json.load(f)
                        if isinstance(existing, dict):
                            data.update(existing)
                except Exception:
                    pass
            # Mettre à jour les filtres depuis l'état courant
            data['forme_juridique'] = self.forme_juridique
            data['en_activite'] = self.en_activite

            # Toujours garder min = max pour recherche par jour
            if self.date_creation_min:
                data['date_creation_min'] = self.date_creation_min
                data['date_creation_max'] = self.date_creation_min  # Toujours identique
            else:
                data.pop('date_creation_min', None)
                data.pop('date_creation_max', None)

            with open(self.config_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"Erreur sauvegarde filtres: {e}")

    def setup_csv(self):
        """Initialiser ou ouvrir le fichier CSV unique pour toutes les exécutions"""
        # Nom de fichier fixe pour persistance (different par type)
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_dir = os.path.join(base_dir, 'csv')
        os.makedirs(csv_dir, exist_ok=True)
        filename = os.path.join(csv_dir, 'company_data_master.csv' if self.scrape_type == "company" else 'sci_data_master.csv')

        fieldnames = [
            # Champs de base
            'nom_societe', 'forme_juridique', 'date_creation', 'activite',
            'code_naf', 'lieu', 'code_postal', 'effectif', 'capital', 'url_entreprise',

            # Informations juridiques détaillées
            'siren', 'siret', 'numero_tva', 'inscription_rcs', 'inscription_rne',
            'numero_rcs', 'capital_social_detaille',

            # Dirigeants (format JSON)
            'dirigeants_json', 'nombre_dirigeants',

            # Enrichissement Yahoo/LinkedIn
            'linkedin_enriched_json',

            # Métadonnées de scraping
            'date_scraping', 'heure_scraping'
        ]

        # Vérifier si le fichier existe déjà
        file_exists = os.path.exists(filename)

        if file_exists:
            # Charger les SIREN existants pour éviter les doublons
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self.existing_sirens = set()
                    row_count = 0
                    for row in reader:
                        if row.get('siren'):
                            self.existing_sirens.add(row['siren'])
                        row_count += 1
                    print(f"📄 Fichier CSV existant trouvé: {filename}")
                    print(f"   📊 {row_count} entreprises déjà présentes")
                    print(f"   🔍 {len(self.existing_sirens)} SIREN uniques chargés")
            except Exception as e:
                self.logger.error(f"Erreur lecture CSV existant: {e}")
                self.existing_sirens = set()

            # Ouvrir en mode append
            self.csv_file = open(filename, 'a', newline='', encoding='utf-8')
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
            print(f"✅ Ajout au fichier CSV existant: {filename}")
        else:
            # Créer un nouveau fichier avec headers
            self.existing_sirens = set()
            self.csv_file = open(filename, 'w', newline='', encoding='utf-8')
            self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=fieldnames)
            self.csv_writer.writeheader()
            print(f"📄 Nouveau fichier CSV créé: {filename}")

        return filename

    async def goto_resilient(self, page, url: str):
        """Aller à une URL avec plusieurs stratégies de chargement et timeouts plus longs."""
        strategies = [
            ("domcontentloaded", 45000),
            ("networkidle", 45000),
            ("domcontentloaded", 60000),
            ("networkidle", 60000),
        ]
        for wait_mode, timeout_ms in strategies:
            try:
                return await page.goto(url, wait_until=wait_mode, timeout=timeout_ms)
            except Exception as e:
                await asyncio.sleep(1)
                continue
        # Dernière tentative sans wait_until
        try:
            return await page.goto(url, timeout=60000)
        except Exception:
            return None

    def build_lead_payload(self, item) -> dict:
        """Construire un lead conforme à l'interface Lead du backend."""
        lead: dict = {}

        # Si enrichissement LinkedIn présent, envoyer le premier résultat
        try:
            if item.get('linkedin_enriched_json'):
                enriched = json.loads(item['linkedin_enriched_json'])
                if isinstance(enriched, list) and enriched:
                    # Prendre le premier dirigeant avec un profil LinkedIn trouvé
                    for dirigeant in enriched:
                        linkedin_url = dirigeant.get('linkedin_url')
                        if linkedin_url and "linkedin.com/in/" in linkedin_url:
                            # Extraire le slug LinkedIn (ex: john-doe-123456)
                            linkedin_slug = linkedin_url.split('linkedin.com/in/')[-1].strip('/ ').split('?')[0]

                            # OBLIGATOIRE: linkedin_id
                            lead['linkedin_id'] = linkedin_slug

                            # Alias pour compatibilité
                            lead['linkedin'] = linkedin_slug

                            # Extraire prénom et nom depuis le nom complet
                            full_name = dirigeant.get('nom') or ''
                            if full_name:
                                # Nettoyer le nom (enlever titres comme M., Mme., Dr.)
                                full_name = re.sub(r'^(M\.|Mme\.|Dr\.|Me\.)\s*', '', full_name, flags=re.IGNORECASE)
                                tokens = [t for t in full_name.split() if t]

                                if len(tokens) >= 2:
                                    # Format classique: Prénom Nom(s)
                                    lead['first_name'] = tokens[0]
                                    lead['last_name'] = ' '.join(tokens[1:])
                                elif tokens:
                                    # Un seul token, on le met en last_name
                                    lead['last_name'] = tokens[0]

                            # Position du dirigeant
                            qualite = dirigeant.get('qualite', '')
                            if qualite:
                                lead['position'] = qualite
                                # Headline peut inclure position + entreprise
                                lead['headline'] = f"{qualite} chez {item.get('nom_societe', '')}" if item.get('nom_societe') else qualite

                            # On prend le premier dirigeant avec LinkedIn trouvé
                            break

                    # Si aucun LinkedIn trouvé, ne pas créer de lead
                    if 'linkedin_id' not in lead:
                        return {}

        except Exception as e:
            self.logger.error(f"Erreur construction lead: {e}")
            return {}

        # Compléter avec les infos d'entreprise
        if item.get('nom_societe'):
            lead['company_name'] = self.fix_encoding(item['nom_societe'])

        if item.get('lieu'):
            lead['raw_address'] = self.fix_encoding(item['lieu'])

        # Extraire le domaine si on a des infos
        # (pourrait être amélioré avec une recherche du site web de l'entreprise)

        # Vérifier qu'on a au minimum linkedin_id
        if 'linkedin_id' not in lead:
            return {}  # Pas de lead sans linkedin_id

        return lead

    def _track_daily_stat(self, key: str, increment: int = 1):
        """Incrementer un compteur dans le fichier de stats quotidiennes."""
        try:
            stats_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs', 'daily_stats.json')
            os.makedirs(os.path.dirname(stats_path), exist_ok=True)
            stats = {}
            today = datetime.now().strftime("%Y-%m-%d")
            if os.path.exists(stats_path):
                with open(stats_path, 'r', encoding='utf-8') as f:
                    stats = json.load(f)
            if stats.get("date") != today:
                stats = {"date": today}
            stats[key] = stats.get(key, 0) + increment
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _get_getsales_list_uuid(self, item):
        """Determiner la liste GetSales en fonction du type de scraping et de l'age de la SCI."""
        if self.scrape_type == "company":
            return os.environ.get("GETSALES_NEW_COMPANY_LIST_UUID", os.environ.get("GETSALES_LIST_UUID"))

        # Mode SCI : determiner si nouvelle ou ancienne
        # On considere qu'une SCI est "nouvelle" si sa date de creation correspond
        # a la date de recherche courante (= SCI recente)
        date_creation = item.get('date_creation', '')
        if date_creation and self.date_creation_min:
            try:
                # date_creation format: DD/MM/YYYY, date_creation_min format: DD-MM-YYYY
                dc = datetime.strptime(date_creation, "%d/%m/%Y").date()
                from datetime import timedelta
                # SCI creee dans les 30 derniers jours = nouvelle
                threshold = datetime.now().date() - timedelta(days=30)
                if dc >= threshold:
                    return os.environ.get("GETSALES_NEW_SCI_LIST_UUID", os.environ.get("GETSALES_LIST_UUID"))
            except Exception:
                pass

        return os.environ.get("GETSALES_OLD_SCI_LIST_UUID", os.environ.get("GETSALES_LIST_UUID"))

    def close_csv(self):
        """Fermer le fichier CSV"""
        if self.csv_file:
            self.csv_file.close()
            print("💾 Fichier CSV fermé")

    def extract_company_data(self, company_html, base_url):
        """Extraire les données d'une entreprise depuis son HTML"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(company_html, 'html.parser')

        item = {}

        # Nom de la société
        nom_selectors = ['.gros-nom', '.petit-nom', '.gros-gros-nom']
        nom = None
        for selector in nom_selectors:
            element = soup.select_one(f'.nom-entreprise {selector}')
            if element:
                nom = element.get_text(strip=True)
                break

        item['nom_societe'] = self.fix_encoding(nom) if nom else ''

        # URL de l'entreprise
        url_element = soup.select_one('.nom-entreprise a')
        if url_element and url_element.get('href'):
            item['url_entreprise'] = base_url + url_element['href']
        else:
            item['url_entreprise'] = ''

        # Initialiser tous les champs de base
        item['forme_juridique'] = ''
        item['date_creation'] = ''
        item['activite'] = ''
        item['code_naf'] = ''
        item['lieu'] = ''
        item['code_postal'] = ''
        item['effectif'] = ''
        item['capital'] = ''

        # Initialiser nouveaux champs
        item['siren'] = ''
        item['siret'] = ''
        item['numero_tva'] = ''
        item['inscription_rcs'] = ''
        item['inscription_rne'] = ''
        item['numero_rcs'] = ''
        item['capital_social_detaille'] = ''
        item['dirigeants_json'] = ''
        item['nombre_dirigeants'] = 0

        # Extraire les informations des sections content
        content_sections = soup.select('div.content')

        for section in content_sections:
            text_content = section.get_text(separator=' ', strip=True)

            # Forme juridique et date de création
            if 'Forme Juridique' in text_content:
                value_element = section.select_one('p.value')
                if value_element:
                    item['forme_juridique'] = self.fix_encoding(value_element.get_text(strip=True))

                date_match = re.search(r'Depuis le (\d{2}/\d{2}/\d{4})', text_content)
                if date_match:
                    item['date_creation'] = date_match.group(1)

            # Activité et Code NAF
            elif 'Activité' in text_content:
                value_element = section.select_one('p.value')
                if value_element:
                    item['activite'] = self.fix_encoding(value_element.get_text(strip=True))

                naf_match = re.search(r'Code NAF : ([A-Z0-9.]+)', text_content)
                if naf_match:
                    item['code_naf'] = naf_match.group(1)

            # Lieu et Code postal
            elif 'Lieu' in text_content:
                value_element = section.select_one('p.value')
                if value_element:
                    item['lieu'] = self.fix_encoding(value_element.get_text(strip=True))

                postal_match = re.search(r'Code postal : (\d+)', text_content)
                if postal_match:
                    item['code_postal'] = postal_match.group(1)

            # Effectif et Capital
            elif 'Effectif' in text_content:
                effectif_match = re.search(r'Effectif : (.+?)(?:Capital|$)', text_content)
                if effectif_match:
                    item['effectif'] = self.fix_encoding(effectif_match.group(1).strip())

                capital_match = re.search(r'Capital : (.+?)$', text_content)
                if capital_match:
                    item['capital'] = self.fix_encoding(capital_match.group(1).strip())

        return item

    async def extract_company_details(self, page, company_url):
        """Extraire les détails d'une entreprise depuis sa page détail"""
        try:
            print(f"  🔍 Extraction des détails de {company_url}")

            # Navigation avec retry et timeouts plus longs
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    # Utiliser domcontentloaded au lieu de networkidle pour éviter les timeouts
                    response = await page.goto(company_url, wait_until="domcontentloaded", timeout=60000)
                    if response and response.status == 200:
                        break
                    elif attempt == max_retries - 1:
                        print(f"    ❌ Erreur de statut: {response.status if response else 'Pas de réponse'}")
                        return self._empty_details()
                except Exception as nav_error:
                    if attempt == max_retries - 1:
                        print(f"    ❌ Erreur de navigation après {max_retries} tentatives")
                        return self._empty_details()
                    print(f"    ⚠️ Tentative {attempt + 1}/{max_retries} échouée, nouvelle tentative...")
                    await asyncio.sleep(5)
                    continue

            await page.wait_for_timeout(2000)

            # Attendre que le contenu soit chargé avec un timeout plus court
            try:
                await page.wait_for_selector('#resume', timeout=5000)
            except:
                print(f"    ⚠️ La page met du temps à charger, on continue...")

            details = {
                'siren': '',
                'siret': '',
                'numero_tva': '',
                'inscription_rcs': '',
                'inscription_rne': '',
                'numero_rcs': '',
                'capital_social_detaille': '',
                'dirigeants_json': '',
                'nombre_dirigeants': 0
            }

            # Extraire les informations juridiques
            try:
                juridiques_section = await page.query_selector('#informations')
                if juridiques_section:
                    juridiques_html = await juridiques_section.inner_html()
                    juridiques_data = self.extract_juridiques(juridiques_html)
                    details.update(juridiques_data)
                else:
                    print(f"    ⚠️ Section informations juridiques non trouvée")
            except Exception as e:
                print(f"    ⚠️ Erreur extraction juridiques: {e}")

            # Extraire les dirigeants - Stratégie principale #dirigeants
            dirigeants_data = []

            try:
                # Attendre spécifiquement la section dirigeants
                await page.wait_for_selector('#dirigeants', timeout=5000)
                dirigeants_section = await page.query_selector('#dirigeants')

                if dirigeants_section:
                    dirigeants_html = await dirigeants_section.inner_html()
                    dirigeants_data = self.extract_dirigeants(dirigeants_html)
                    if dirigeants_data:
                        print(f"    ✅ {len(dirigeants_data)} dirigeant(s) trouvé(s)")
                    else:
                        print(f"    ⚠️ Section #dirigeants présente mais vide")
                else:
                    print(f"    ⚠️ Pas de section #dirigeants trouvée")
            except Exception as e:
                print(f"    ⚠️ Impossible de récupérer les dirigeants (timeout ou erreur)")

            # Mettre à jour les détails
            if dirigeants_data:
                # Dédoublon si nécessaire
                unique_dirigeants = []
                seen = set()
                for d in dirigeants_data:
                    if d['nom'] not in seen:
                        unique_dirigeants.append(d)
                        seen.add(d['nom'])

                details['dirigeants_json'] = json.dumps(unique_dirigeants, ensure_ascii=False)
                details['nombre_dirigeants'] = len(unique_dirigeants)
                print(f"    🎯 Total: {len(unique_dirigeants)} dirigeant(s) unique(s)")
                for i, d in enumerate(unique_dirigeants, 1):
                    print(f"       {i}. {d['nom']} ({d.get('qualite', 'Gérant')})")
            else:
                # Une SCI a forcément des dirigeants - on le signale comme une anomalie
                details['dirigeants_json'] = ''
                details['nombre_dirigeants'] = 0
                print(f"    ❌ ANOMALIE: Aucun dirigeant trouvé (toute SCI doit avoir au moins un gérant)")

            return details

        except Exception as e:
            print(f"    ❌ Erreur lors de l'extraction des détails: {e}")
            return self._empty_details()

    def _empty_details(self):
        """Retourne un dictionnaire vide pour les détails"""
        return {
            'siren': '',
            'siret': '',
            'numero_tva': '',
            'inscription_rcs': '',
            'inscription_rne': '',
            'numero_rcs': '',
            'capital_social_detaille': '',
            'dirigeants_json': '',
            'nombre_dirigeants': 0
        }

    # =====================
    # Enrichissement Yahoo
    # =====================
    def clean_name_for_search(self, name: str) -> str:
        if not name:
            return ''
        # supprimer parenthèses et normaliser espaces
        name = re.sub(r'\([^)]*\)', '', name)
        name = re.sub(r'[^\w\s-]', '', name)
        return ' '.join(name.split()).strip()

    def clean_linkedin_url(self, url: str):
        if not url:
            return None
        url = unquote(url)
        if "/url?q=" in url:
            url = url.split("/url?q=")[-1].split("&")[0]
        # Accepter également l'ancien format /pub/
        if ("linkedin.com/in/" not in url) and ("linkedin.com/pub/" not in url):
            # Pas un profil LinkedIn
            return None
        if "?" in url:
            url = url.split("?")[0]
        if not url.startswith("http"):
            url = "https://" + url.lstrip("/")
        self.logger.debug("URL LinkedIn nettoyée: %s", url)
        return url

    def resolve_search_result_href(self, href: str) -> str:
        """Résoudre un href de moteur de recherche vers son URL cible réelle.

        Supporte les redirections typiques (Bing/Google/Qwant). Retourne l'URL brute si aucune redirection.
        """
        if not href:
            return None
        try:
            parsed = urlparse(href)
            raw = href
            # Cas 1: lien direct vers LinkedIn
            if parsed.netloc and "linkedin.com" in parsed.netloc:
                return raw

            # Cas 2: liens de redirection (Bing/Google/Qwant/Yahoo)
            query = parse_qs(parsed.query)
            # Essayer toutes les clés usuelles
            candidate_keys = [
                "url", "u", "r", "target", "redirect", "link", "RU", "ru", "to", "l"
            ]
            for key in candidate_keys:
                if key in query and len(query[key]) > 0:
                    candidate = unquote(query[key][0])
                    if "linkedin.com" in candidate:
                        return candidate

            # Cas 3: chemins spéciaux contenant redirect
            if any(seg in (parsed.path or "") for seg in ["/redirect", "/r/", "/l/", "/ck/a", "/url"]):
                for key in candidate_keys:
                    if key in query and len(query[key]) > 0:
                        candidate = unquote(query[key][0])
                        if "linkedin.com" in candidate:
                            return candidate
        except Exception:
            pass
        return href

    def generate_search_queries(self, dirigeant, company_info):
        name = self.clean_name_for_search(dirigeant.get('nom', ''))
        if not name:
            return []

        # Une seule requête simple et efficace
        return [f'site:linkedin.com "{name}"']

    async def search_yahoo_for_linkedin(self, context, query):
        """Recherche via Yahoo avec requête site:linkedin.com."""
        try:
            if self.google_page is None:
                self.google_page = await context.new_page()

            yahoo_url = f"https://fr.search.yahoo.com/search?p={quote(query)}"
            self.logger.debug("Yahoo URL: %s", yahoo_url)

            response = await self.google_page.goto(yahoo_url, wait_until="domcontentloaded", timeout=30000)
            await self.google_page.wait_for_timeout(2500)

            # Yahoo utilise différents sélecteurs
            selectors = [
                'a[href*="linkedin.com/in/"]',
                'a[href*="linkedin.com/pub/"]',
                '.compTitle a',
                'h3.title a',
                '.algo-sr a',
                '.ac-algo a',
                'div.dd a'
            ]

            linkedin_urls = []
            for selector in selectors:
                try:
                    links = await self.google_page.query_selector_all(selector)
                    for link in links:
                        href = await link.get_attribute('href')
                        if href and "linkedin" in href.lower():
                            # Yahoo encode les URLs différemment
                            if "r.search.yahoo.com" in href or "/RU=" in href:
                                # Extraire l'URL réelle
                                import re
                                match = re.search(r'/RU=([^/]+)/', href)
                                if match:
                                    href = unquote(match.group(1))

                            resolved = self.resolve_search_result_href(href)
                            if resolved and "linkedin.com/in/" in resolved:
                                clean = self.clean_linkedin_url(resolved)
                                if clean:
                                    return clean
                except:
                    continue

            self.logger.info("Aucun profil LinkedIn trouvé pour: %s", query)
            return None

        except Exception as e:
            self.logger.error("Recherche échouée: %s", e)
            return None

    async def enrich_with_yahoo(self, page, item, company_info):
        """Enrichir l'item avec des URLs LinkedIn via Yahoo pour TOUS les dirigeants."""
        enriched = []
        try:
            self.logger.info("\n" + "=" * 70)
            self.logger.info("🔍 ENRICHISSEMENT YAHOO pour '%s' (%s)", item.get('nom_societe', ''), company_info.get('lieu', ''))
            self.logger.info("=" * 70)

            dirigeants = []
            if item.get('dirigeants_json'):
                try:
                    dirigeants = json.loads(item['dirigeants_json'])
                except Exception:
                    self.logger.exception("Échec du parsing de dirigeants_json pour '%s'", item.get('nom_societe', ''))
                    dirigeants = []

            if not dirigeants:
                self.logger.info("❌ Aucun dirigeant à enrichir pour '%s'", item.get('nom_societe', ''))
            else:
                self.logger.info("👥 %d dirigeant(s) à rechercher sur LinkedIn via Yahoo", len(dirigeants))

            # Traiter TOUS les dirigeants
            for idx, dirigeant in enumerate(dirigeants, 1):
                nom_dirigeant = dirigeant.get('nom', '')
                qualite_dirigeant = dirigeant.get('qualite', '')

                self.logger.info("\n🔎 Dirigeant %d/%d: %s (%s)", idx, len(dirigeants), nom_dirigeant, qualite_dirigeant)
                self.logger.info("-" * 50)

                # Une seule requête par dirigeant
                query = self.generate_search_queries(dirigeant, company_info)[0]
                self.enrichment_queries_count += 1

                # Cooldown anti-bot périodique
                if self.enrichment_queries_count % 15 == 0:
                    self.logger.info("⏸ Cooldown anti-bot: pause 30s après %d requêtes", self.enrichment_queries_count)
                    await asyncio.sleep(30)

                # Pause pour paraître plus humain
                await asyncio.sleep(2)

                self.logger.info("  Recherche: %s", query)

                url = await self.search_yahoo_for_linkedin(page.context, query)

                if url:
                    enriched.append({
                        'nom': nom_dirigeant,
                        'qualite': qualite_dirigeant,
                        'query': query,
                        'linkedin_url': url
                    })
                    self.logger.info("✅ TROUVÉ ! LinkedIn: %s", url)
                else:
                    self.logger.info("  ❌ Pas de résultat")
                    enriched.append({
                        'nom': nom_dirigeant,
                        'qualite': qualite_dirigeant,
                        'query': query,
                        'linkedin_url': ''
                    })

                # Pause entre dirigeants pour éviter la détection
                if idx < len(dirigeants):  # Pas de pause après le dernier
                    self.logger.info("⏱ Pause 3s avant le prochain dirigeant...")
                    await asyncio.sleep(3)
        except Exception:
            self.logger.exception("Erreur pendant l'enrichissement pour '%s'", item.get('nom_societe', ''))

        # Résumé final pour cette entreprise
        self.logger.info("\n" + "=" * 70)
        if enriched:
            item['linkedin_enriched_json'] = json.dumps(enriched, ensure_ascii=False)
            profiles_found = [e for e in enriched if e.get('linkedin_url')]
            self.logger.info("🎯 RÉSULTAT ENRICHISSEMENT pour '%s':", item.get('nom_societe', ''))
            self.logger.info("  - Dirigeants totaux: %d", len(enriched))
            self.logger.info("  - Profils LinkedIn trouvés: %d/%d", len(profiles_found), len(enriched))

            if profiles_found:
                self.logger.info("  - Profils trouvés:")
                for p in profiles_found:
                    self.logger.info("    • %s: %s", p['nom'], p['linkedin_url'])
        else:
            item['linkedin_enriched_json'] = ''
            self.logger.info("🎯 Enrichissement Yahoo terminé pour '%s' – aucun dirigeant à enrichir", item.get('nom_societe', ''))
        self.logger.info("=" * 70 + "\n")

    def extract_juridiques(self, html_content):
        """Extraire les informations juridiques"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        juridiques = {}

        # Parcourir les lignes du tableau
        rows = soup.select('table tbody tr')
        for row in rows:
            th = row.select_one('th')
            td = row.select_one('td')

            if th and td:
                label = th.get_text(strip=True).lower()
                value = td.get_text(strip=True)
                value = self.fix_encoding(value)

                if 'siren' in label and 'siret' not in label:
                    # Extraire seulement les chiffres pour SIREN
                    siren_match = re.search(r'(\d{3}\s?\d{3}\s?\d{3})', value)
                    if siren_match:
                        juridiques['siren'] = siren_match.group(1).replace(' ', '')

                elif 'siret' in label:
                    # Extraire seulement les chiffres pour SIRET
                    siret_match = re.search(r'(\d{3}\s?\d{3}\s?\d{3}\s?\d{5})', value)
                    if siret_match:
                        juridiques['siret'] = siret_match.group(1).replace(' ', '')

                elif 'tva' in label:
                    # Extraire le numéro de TVA
                    tva_match = re.search(r'(FR\d+)', value)
                    if tva_match:
                        juridiques['numero_tva'] = tva_match.group(1)

                elif 'inscription au rcs' in label:
                    juridiques['inscription_rcs'] = value

                elif 'inscription au rne' in label:
                    juridiques['inscription_rne'] = value

                elif 'numéro rcs' in label:
                    juridiques['numero_rcs'] = value

                elif 'capital social' in label:
                    juridiques['capital_social_detaille'] = value

        return juridiques

    def extract_dirigeants(self, html_content):
        """Extraire les informations des dirigeants (personnes physiques uniquement)"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')

        dirigeants = []

        # Trouver tous les dirigeants
        dirigeants_items = soup.select('li.dirigeant')

        for dirigeant_item in dirigeants_items:
            dirigeant = {}

            # Nom du dirigeant
            nom_element = dirigeant_item.select_one('.nom a')
            if nom_element:
                nom_complet = nom_element.get_text(strip=True)
                nom_complet = self.fix_encoding(nom_complet)

                # Vérifier s'il s'agit d'une personne physique (pas d'une société)
                # Les personnes physiques ont généralement prénom + nom
                if self.is_personne_physique(nom_complet):
                    dirigeant['nom'] = nom_complet

                    # Extraire nom d'usage si présent
                    nom_usage = dirigeant_item.select_one('.personne-nom-usage')
                    if nom_usage:
                        usage_text = nom_usage.get_text(strip=True)
                        dirigeant['nom_usage'] = self.fix_encoding(usage_text)

                    # Qualité/fonction
                    qualite_element = dirigeant_item.select_one('.qualite')
                    if qualite_element:
                        dirigeant['qualite'] = self.fix_encoding(qualite_element.get_text(strip=True))

                    # Âge et date de naissance
                    age_element = dirigeant_item.select_one('.age-siren span')
                    if age_element:
                        age_text = age_element.get_text(strip=True)
                        age_match = re.search(r'(\d+)\s*ans.*?(\d{2}/\d{4})', age_text)
                        if age_match:
                            dirigeant['age'] = age_match.group(1)
                            dirigeant['date_naissance'] = age_match.group(2)

                    # Date de début de fonction
                    date_element = dirigeant_item.select_one('.dirigeant-right .date')
                    if date_element:
                        date_text = date_element.get_text(strip=True)
                        date_match = re.search(r'Depuis le (\d{2}/\d{2}/\d{4})', date_text)
                        if date_match:
                            dirigeant['date_debut_fonction'] = date_match.group(1)

                    dirigeants.append(dirigeant)

        return dirigeants

    def is_personne_physique(self, nom):
        """Déterminer si c'est une personne physique ou une société"""
        # Indicateurs de sociétés
        societe_keywords = [
            'SCI', 'SARL', 'SA', 'SAS', 'EURL', 'SNC', 'SASU',
            'Société', 'Établissement', 'Groupe', 'Holding',
            'Association', 'Fondation', 'GIE'
        ]

        nom_upper = nom.upper()
        for keyword in societe_keywords:
            if keyword.upper() in nom_upper:
                return False

        # Si le nom contient typiquement prénom + nom (2-3 mots), c'est probablement une personne physique
        mots = nom.split()
        if 2 <= len(mots) <= 4:
            return True

        return False

    async def scrape_page(self, page, url, page_num):
        """Scraper une page"""
        print(f"🔄 Page {page_num}: {url}")

        try:
            response = await self.goto_resilient(page, url)

            if not response or response.status != 200:
                print(f"❌ Erreur de statut: {response.status}")
                return 0

            # Attendre que la page soit chargée
            try:
                await page.wait_for_selector('div.container-resultat', timeout=10000)
            except Exception:
                await page.wait_for_timeout(3000)

            # Trouver les entreprises et extraire les données de base en une fois
            companies = await page.query_selector_all('div.container-resultat')
            print(f"🏢 {len(companies)} entreprises trouvées")

            if not companies:
                return 0

            # Extraire d'abord toutes les données de base des entreprises
            companies_data = []
            for company in companies:
                try:
                    company_html = await company.inner_html()
                    item = self.extract_company_data(company_html, "https://www.pappers.fr")
                    if item['nom_societe'] and item['url_entreprise']:
                        companies_data.append(item)
                except Exception as e:
                    print(f"  ⚠️ Erreur extraction entreprise: {e}")
                    continue

            print(f"📋 {len(companies_data)} entreprises valides trouvées")

            # Maintenant extraire les détails de chaque entreprise
            success_count = 0

            for item in companies_data:
                try:
                    # Extraire les détails de l'entreprise
                    details = await self.extract_company_details(page, item['url_entreprise'])

                    # Fusionner les données
                    item.update(details)

                    # Enrichissement Yahoo (LinkedIn) sur 1-2 dirigeants
                    company_info = {
                        'nom_societe': item.get('nom_societe', ''),
                        'lieu': item.get('lieu', '')
                    }
                    await self.enrich_with_yahoo(page, item, company_info)

                    # Upsert lead si enrichissement trouvé
                    if item.get('linkedin_enriched_json'):
                        try:
                            lead_payload = self.build_lead_payload(item)
                            if lead_payload:
                                getsales_api_key = os.environ.get("GETSALES_API_KEY")
                                if getsales_api_key:
                                    # Determiner la bonne liste GetSales selon le type
                                    list_uuid = self._get_getsales_list_uuid(item)

                                    gs_service = GetSalesService(
                                        api_key=getsales_api_key,
                                        base_url=os.environ.get("GETSALES_BASE_URL", "https://amazing.getsales.io"),
                                        list_uuid=list_uuid,
                                        debug=False
                                    )

                                    resp = gs_service.process_lead(lead_payload)
                                    status = resp.get('status')
                                    list_label = "NEW_SCI" if list_uuid == os.environ.get("GETSALES_NEW_SCI_LIST_UUID") else \
                                                 "OLD_SCI" if list_uuid == os.environ.get("GETSALES_OLD_SCI_LIST_UUID") else \
                                                 "COMPANY" if list_uuid == os.environ.get("GETSALES_NEW_COMPANY_LIST_UUID") else "DEFAULT"
                                    print(f"    Lead GetSales [{list_label}] ({item.get('nom_societe','')}) → {lead_payload.get('linkedin','')} status={status}")

                                    # Ajouter a la file d'enrichissement LinkedIn
                                    if status == "created":
                                        lead_data = resp.get("data", {})
                                        lead_inner = lead_data.get("lead", {})
                                        lead_uuid = lead_inner.get("uuid")
                                        if lead_uuid:
                                            try:
                                                gs_service.queue_enrichment(uuids=[lead_uuid])
                                                print(f"    Enrichissement LinkedIn en file pour {lead_payload.get('linkedin','')}")
                                            except Exception as enrich_err:
                                                print(f"    ⚠️ Erreur file enrichissement: {str(enrich_err)[:80]}")

                                    # Tracker stats quotidiennes
                                    self._track_daily_stat("pappers_getsales_sent")

                                    # reset compteur d'échecs
                                    self.consecutive_send_failures = 0
                                else:
                                    print(f"    ⚠️ GETSALES_API_KEY non configurée, lead ignoré")
                        except Exception as send_err:
                            # Formater l'erreur de manière plus lisible
                            error_msg = str(send_err)
                            if "HTTPError" in error_msg:
                                # Extraire le code d'erreur HTTP
                                import re
                                match = re.search(r'HTTPError (\d+)', error_msg)
                                if match:
                                    code = match.group(1)
                                    if code == "404":
                                        print(f"    ⚠️ Erreur API: Endpoint introuvable (404)")
                                        print(f"       Vérifiez la variable LEAD_UPSERT_URL")
                                    elif code == "401":
                                        print(f"    ⚠️ Erreur API: Authentification échouée (401)")
                                        print(f"       Vérifiez la variable LEAD_API_KEY")
                                    elif code == "403":
                                        print(f"    ⚠️ Erreur API: Accès refusé (403)")
                                    elif code == "500":
                                        print(f"    ⚠️ Erreur API: Erreur serveur (500)")
                                    else:
                                        print(f"    ⚠️ Erreur API: HTTP {code}")
                                else:
                                    print(f"    ⚠️ Erreur envoi lead (HTTP)")
                            else:
                                # Autres types d'erreurs
                                if "Connection" in error_msg:
                                    print(f"    ⚠️ Erreur connexion au serveur")
                                elif "Timeout" in error_msg:
                                    print(f"    ⚠️ Timeout lors de l'envoi")
                                else:
                                    # Afficher seulement les 80 premiers caractères de l'erreur
                                    short_error = error_msg[:80] + "..." if len(error_msg) > 80 else error_msg
                                    print(f"    ⚠️ Erreur: {short_error}")

                            self.consecutive_send_failures += 1
                            if self.consecutive_send_failures >= 3:
                                print(f"    ⛔ 3 échecs consécutifs d'envoi. Sauvegarde de l'état et arrêt.")
                                self.save_resume_page(page_num, status='interrompu_erreurs')
                                self.stop_requested = True
                                # on écrit quand même l'item dans le CSV puis on sort
                    
                    # Vérifier les doublons avant d'écrire
                    siren = item.get('siren', '')
                    skip_reason = None

                    if siren and siren in self.existing_sirens:
                        skip_reason = "doublon SIREN"

                    if skip_reason:
                        print(f"⏭️  Entreprise ignorée ({skip_reason}): {item['nom_societe']} - SIREN: {siren}")
                    else:
                        # Ajouter les métadonnées de scraping
                        now = datetime.now()
                        item['date_scraping'] = now.strftime("%Y-%m-%d")
                        item['heure_scraping'] = now.strftime("%H:%M:%S")

                        # Écrire dans le CSV
                        self.csv_writer.writerow(item)
                        self.csv_file.flush()  # Forcer l'écriture immédiate

                        # Ajouter à la liste des SIREN traités
                        if siren:
                            self.existing_sirens.add(siren)

                        success_count += 1
                        self.total_companies += 1
                        self._track_daily_stat("pappers_scraped")
                    
                    # Compter les profils LinkedIn trouvés
                    linkedin_count = 0
                    if item.get('linkedin_enriched_json'):
                        try:
                            enriched_data = json.loads(item['linkedin_enriched_json'])
                            linkedin_count = sum(1 for e in enriched_data if e.get('linkedin_url'))
                        except:
                            pass

                        dirigeants_info = f" - {item['nombre_dirigeants']} dirigeant(s)" if item['nombre_dirigeants'] > 0 else ""
                        linkedin_info = f" - {linkedin_count} LinkedIn" if linkedin_count > 0 else ""
                        print(f"✅ {self.total_companies:3d}. {item['nom_societe']} - {item['lieu']}{dirigeants_info}{linkedin_info}")
                    
                    # Pause entre les entreprises pour éviter la surcharge
                    await asyncio.sleep(2)
                    
                except Exception as e:
                    print(f"  ❌ Erreur traitement {item['nom_societe']}: {e}")
                    continue
            
            return success_count
            
        except Exception as e:
            print(f"❌ Erreur: {e}")
            return 0
    
    def _record_day_stats(self):
        """Enregistre les stats du jour courant dans days_stats et reset le compteur."""
        if self.current_day_count > 0 or self.date_creation_min:
            self.days_stats.append({
                "date": self.date_creation_min or "inconnue",
                "count": self.current_day_count,
            })
        self.current_day_count = 0

    async def _scrape_date(self, page, page_num_start=1):
        """Scrape toutes les pages pour la date courante (date_creation_min).

        Retourne le nombre total d'entreprises trouvées pour cette date.
        """
        page_num = page_num_start
        pages_processed = 0
        date_total = 0

        while True:
            self.current_page_num = page_num
            try:
                self.save_resume_page(page_num)
            except Exception:
                pass

            url = self.build_search_url(page_num)
            if pages_processed == 0:
                print(f"🗓️  Filtres date: min={self.date_creation_min or '-'} max={self.date_creation_max or '-'}")

            companies_found = await self.scrape_page(page, url, page_num)

            if self.stop_requested:
                print(f"⛔ Arrêt demandé à la page {page_num}")
                try:
                    self.save_resume_page(page_num, status='interrompu_erreurs')
                except Exception:
                    pass
                break

            if companies_found == 0:
                print(f"📅 Fin de pagination pour la date {self.date_creation_min} (page {page_num})")
                break

            date_total += companies_found
            self.current_day_count += companies_found
            pages_processed += 1

            print(f"\n✅ Page {page_num} terminée - {companies_found} entreprises")
            print(f"   📊 Total cumulé: {self.total_companies}")

            if self.max_pages and pages_processed >= self.max_pages:
                print(f"🏁 Limite de {self.max_pages} pages atteinte")
                break

            try:
                self.save_resume_page(page_num + 1, status='en_cours')
            except Exception:
                pass
            page_num += 1

            print("⏳ Pause 8 secondes...")
            await asyncio.sleep(8)

        return date_total

    async def run(self, mode="default", send_email=False, email_to=None):
        """Lancer le scraping.

        Args:
            mode: "default" (comportement classique infini) ou "production"
                  (jour courant + 2 jours depuis la dernière position sauvegardée)
            send_email: Envoyer un rapport par email via Resend à la fin
            email_to: Liste d'adresses email destinataires du rapport
        """
        self.start_time = time.time()

        type_label = "SOCIETES (SASU/SAS/EURL/SARL)" if self.scrape_type == "company" else "SCI"
        print(f"🕷️ SCRAPER {type_label} PAPPERS.FR - VERSION AMÉLIORÉE")
        print("=" * 70)
        print(f"🚀 Mode: {mode.upper()}")
        print(f"📋 Type: {type_label} (forme juridique: {self.forme_juridique})")
        print(f"📄 Pages max par date: {'Illimité' if self.max_pages is None else self.max_pages}")
        print(f"👁️  Mode headless: {self.headless}")
        print(f"📧 Envoi email: {'Oui' if send_email else 'Non'}")
        print()

        filename = self.setup_csv()

        # Vérification de la configuration GetSales au démarrage
        getsales_key = os.environ.get("GETSALES_API_KEY")
        if getsales_key:
            print(f"✅ Configuration GetSales Direct détectée (Clé API présente)")
        else:
            print(f"⚠️  GETSALES_API_KEY non trouvée - les leads ne seront pas envoyés")

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=self.headless,
                    args=[
                        "--no-sandbox",
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage"
                    ]
                )

                context_kwargs = dict(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    locale="fr-FR",
                    timezone_id="Europe/Paris",
                )
                try:
                    import os as _os
                    import json as _json
                    if _os.path.exists(self.storage_state_path) and _os.path.getsize(self.storage_state_path) > 0:
                        with open(self.storage_state_path, 'r', encoding='utf-8') as f:
                            _json.load(f)
                        context_kwargs["storage_state"] = self.storage_state_path
                except Exception as e:
                    self.logger.debug(f"Impossible de charger le storage_state: {e}")

                context = await browser.new_context(**context_kwargs)
                page = await context.new_page()

                if mode == "production":
                    await self._run_production(page)
                else:
                    await self._run_default(page)

                # Sauvegarder l'état (cookies)
                try:
                    await context.storage_state(path=self.storage_state_path)
                except Exception:
                    pass
                await browser.close()

        except KeyboardInterrupt:
            print("\n" + "="*70)
            print("⚠️ SCRAPING INTERROMPU PAR L'UTILISATEUR")
            print("="*70)
            current_page = self.current_page_num if hasattr(self, 'current_page_num') else 1
            print(f"📍 Interrompu à la page: {current_page}")
            print(f"📅 Sur la date: {self.date_creation_min}")
            print(f"🏢 Total entreprises: {self.total_companies}")
            try:
                self.save_resume_page(current_page, status='interrompu_utilisateur')
            except Exception:
                pass
        except Exception as e:
            import traceback
            print(f"\n❌ ERREUR GÉNÉRALE: {e}")
            traceback.print_exc()
            current_page = self.current_page_num if hasattr(self, 'current_page_num') else 1
            try:
                self.save_resume_page(current_page, status='erreur')
            except Exception:
                pass

        finally:
            self.close_csv()

            duration = time.time() - self.start_time
            print("\n" + "=" * 70)
            print("📊 STATISTIQUES FINALES")
            print("=" * 70)
            print(f"🏢 Total d'entreprises extraites cette session: {self.total_companies}")
            print(f"⏱️  Durée du scraping: {duration:.1f} secondes")
            print(f"📄 Fichier CSV: {filename}")
            if self.total_companies > 0 and duration > 0:
                print(f"🎯 Moyenne: {self.total_companies/duration*60:.1f} entreprises/minute")

            if self.days_stats:
                print(f"\n📅 DÉTAIL PAR JOUR:")
                for day in self.days_stats:
                    print(f"   {day['date']}: {day['count']} SCI")

            print(f"\n💾 Configuration finale dans: {self.config_file_path}")
            print("✅ SCRAPING TERMINÉ")

            # Envoi du rapport par email
            if send_email and self.days_stats:
                try:
                    resend_svc = ResendService()
                    recipients = email_to or [os.getenv("RESEND_TO_EMAIL", "")]
                    recipients = [r for r in recipients if r]
                    if recipients:
                        resend_svc.send_scraping_report(
                            to=recipients,
                            date_scraped=datetime.now().strftime("%d-%m-%Y %H:%M"),
                            total_sci=self.total_companies,
                            days_scraped=self.days_stats,
                            duration_seconds=duration,
                        )
                        print(f"📧 Rapport envoyé à: {', '.join(recipients)}")
                    else:
                        print("⚠️ Pas de destinataire email configuré (RESEND_TO_EMAIL)")
                except Exception as e:
                    print(f"⚠️ Erreur envoi rapport email: {e}")

    async def _run_production(self, page):
        """Mode production: scrape la veille (et reprend les anciens jours pour les SCI)."""
        from datetime import timedelta

        # --- Charger la position sauvegardée AVANT de la modifier ---
        self.load_search_filters()
        saved_date = self.date_creation_min  # ex: "24-03-2026"
        saved_resume_page = max(1, int(self.load_resume_page()))

        # --- Jour 1 : la veille ---
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%d-%m-%Y")

        resume_dates = []
        
        # Logique différente selon le type d'entreprise
        if self.scrape_type == "company":
            # Pour les entreprises, on ne scrape QUE la veille
            print("\n" + "="*70)
            print("🚀 MODE PRODUCTION: 1 jour à scraper (Sociétés)")
            print("="*70)
            print(f"   Jour 1: {yesterday} (veille)")
            print("="*70)
        else:
            # Pour les SCI, on scrape sur 3 jours (la veille + reprise config)
            if saved_date and saved_date != yesterday:
                resume_dates.append(saved_date)
                d = self._parse_fr_date(saved_date)
                if d:
                    resume_dates.append(self._format_fr_date(d - timedelta(days=1)))
            else:
                # Pas de config ou config = veille → on recule depuis la veille
                d = self._parse_fr_date(yesterday)
                if d:
                    resume_dates.append(self._format_fr_date(d - timedelta(days=1)))
                    resume_dates.append(self._format_fr_date(d - timedelta(days=2)))

            print("\n" + "="*70)
            print("🚀 MODE PRODUCTION: 3 jours à scraper (SCI)")
            print("="*70)
            print(f"   Jour 1: {yesterday} (veille)")
            for i, d in enumerate(resume_dates, 2):
                label = "(reprise config)" if i == 2 and saved_date and saved_date != yesterday else ""
                print(f"   Jour {i}: {d} {label}")
            print("="*70)

        # --- Scraper jour 1 : veille ---
        print(f"\n{'='*70}")
        total_days = 3 if self.scrape_type == "sci" else 1
        print(f"📅 JOUR 1/{total_days}: {yesterday} (veille)")
        print(f"{'='*70}")

        self.date_creation_min = yesterday
        self.date_creation_max = yesterday
        self.save_filters_to_config()

        self.current_day_count = 0
        await self._scrape_date(page, page_num_start=1)
        self._record_day_stats()

        # --- Scraper jours suivants si SCI ---
        if self.scrape_type == "sci" and resume_dates:
            for i, date_str in enumerate(resume_dates, 2):
                if self.stop_requested:
                    break

                print(f"\n{'='*70}")
                print(f"📅 JOUR {i}/3: {date_str}")
                print(f"{'='*70}")

                self.date_creation_min = date_str
                self.date_creation_max = date_str
                self.save_filters_to_config()
                self.current_day_count = 0

                start_p = saved_resume_page if (i == 2 and saved_date and saved_date != yesterday) else 1
                await self._scrape_date(page, page_num_start=start_p)
                self._record_day_stats()

                await asyncio.sleep(2)

        # Sauvegarder la position finale pour la prochaine reprise
        try:
            self.save_filters_to_config()
            self.save_resume_page(1, status='termine_production')
        except Exception:
            pass

    async def _run_default(self, page):
        """Mode par défaut: comportement classique (infini, recule jour par jour)."""
        self.load_search_filters()

        if not self.date_creation_min:
            today = datetime.now()
            self.date_creation_min = today.strftime("%d-%m-%Y")
            self.date_creation_max = self.date_creation_min
            self.logger.info(f"📅 Pas de date définie, démarrage avec aujourd'hui: {self.date_creation_min}")
            self.save_filters_to_config()
        page_num = max(1, int(self.load_resume_page()))

        print("\n" + "="*70)
        print("📋 CONFIGURATION DU SCRAPING")
        print("="*70)
        print(f"📄 Page de démarrage: {page_num}")
        print(f"📅 Date de recherche: {self.date_creation_min or 'Non définie'}")
        fj_label = "SASU/SAS/EURL/SARL" if self.scrape_type == "company" else "SCI"
        print(f"🏢 Forme juridique: {self.forme_juridique} ({fj_label})")
        print(f"✅ En activité uniquement: {self.en_activite}")
        if page_num > 1:
            print(f"\n🔁 REPRISE à la page {page_num} pour la date {self.date_creation_min}")
        print("="*70 + "\n")

        pages_processed = 0
        while True:
            self.current_page_num = page_num
            try:
                self.save_resume_page(page_num)
            except Exception:
                pass

            url = self.build_search_url(page_num)
            if pages_processed == 0 and (self.date_creation_min or self.date_creation_max):
                print(f"🗓️  Filtres date: min={self.date_creation_min or '-'} max={self.date_creation_max or '-'}")

            companies_found = await self.scrape_page(page, url, page_num)

            if self.stop_requested:
                print(f"⛔ Arrêt demandé à la page {page_num}")
                try:
                    self.save_resume_page(page_num, status='interrompu_erreurs')
                except Exception:
                    pass
                break

            if companies_found == 0:
                print(f"📅 Fin de pagination pour {self.date_creation_min}")

                # Enregistrer les stats du jour
                self._record_day_stats()

                old_date = self.date_creation_min
                self.shift_date_filters_by_days(-1)

                print(f"🔄 Passage: {old_date} -> {self.date_creation_min}")

                try:
                    self.save_filters_to_config()
                    self.save_resume_page(1, status='changement_date')
                except Exception:
                    pass
                page_num = 1
                self.current_day_count = 0
                await asyncio.sleep(2)
                continue

            self.current_day_count += companies_found
            pages_processed += 1

            print(f"\n✅ Page {page_num} terminée - {companies_found} entreprises")
            print(f"   📊 Total cumulé: {self.total_companies}")

            if self.max_pages and pages_processed >= self.max_pages:
                self._record_day_stats()
                print(f"🏁 Limite de {self.max_pages} pages atteinte")
                break

            try:
                self.save_resume_page(page_num + 1, status='en_cours')
            except Exception:
                pass
            page_num += 1

            print("⏳ Pause 8 secondes...")
            await asyncio.sleep(8)


def is_docker():
    """Détecte si on tourne dans un conteneur Docker."""
    return os.path.exists("/.dockerenv") or os.environ.get("DOCKER", "").lower() in ("1", "true")


async def main():
    """Point d'entrée principal"""
    import argparse

    parser = argparse.ArgumentParser(description="Scraper Pappers.fr (SCI ou Societes)")
    parser.add_argument(
        "--mode",
        choices=["test", "visible", "unlimited", "production", "custom"],
        default=None,
        help="Mode de scraping (skip le menu interactif)",
    )
    parser.add_argument(
        "--type",
        choices=["sci", "company"],
        default="sci",
        help="Type de scraping: 'sci' pour SCI (6540), 'company' pour SASU/SAS/EURL/SARL (5720,5710,5498,5499)",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="Envoyer un rapport par email via Resend à la fin du scraping",
    )
    parser.add_argument(
        "--email-to",
        nargs="+",
        help="Adresses email destinataires du rapport (par défaut: RESEND_TO_EMAIL)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Nombre max de pages par date (écrase la valeur du mode choisi)",
    )

    args = parser.parse_args()
    scrape_type = args.type

    # --- Docker: mode production headless par défaut ---
    if args.mode is None and is_docker():
        print("🐳 Docker détecté → mode production headless automatique")
        args.mode = "production"
        if not args.send_email:
            # En Docker, activer l'email si RESEND_API_KEY est configuré
            if os.environ.get("RESEND_API_KEY"):
                args.send_email = True

    # --- Si pas de mode passé en argument, afficher le menu interactif ---
    if args.mode is None:
        type_label = "SOCIETES" if scrape_type == "company" else "SCI"
        print(f"Configuration du scraping amélioré ({type_label}):")
        print("1. Mode test (headless, 2 pages)")
        print("2. Mode visible (avec navigateur, 2 pages)")
        print("3. Mode illimité (headless, pas de limite)")
        print("4. Configuration personnalisée")
        print("5. Mode production (jour courant + 2 jours précédents)")

        try:
            choice = input("\nChoisissez une option (1-5): ").strip()
        except KeyboardInterrupt:
            print("\nAu revoir !")
            return

        mode_map = {"1": "test", "2": "visible", "3": "unlimited", "4": "custom", "5": "production"}
        args.mode = mode_map.get(choice, "test")

        # Demander l'envoi d'email pour le mode production
        if args.mode == "production" and not args.send_email:
            try:
                send = input("Envoyer un rapport par email ? (o/n): ").strip().lower()
                args.send_email = send.startswith("o")
            except KeyboardInterrupt:
                pass

    type_label = "SOCIETES (SASU/SAS/EURL/SARL)" if scrape_type == "company" else "SCI"
    print(f"📋 Type de scraping: {type_label}")

    # --- Construction du scraper selon le mode ---
    if args.mode == "test":
        scraper = SciScraperEnhanced(headless=True, max_pages=args.max_pages or 2, scrape_type=scrape_type)
        run_mode = "default"
    elif args.mode == "visible":
        scraper = SciScraperEnhanced(headless=False, max_pages=args.max_pages or 2, scrape_type=scrape_type)
        run_mode = "default"
    elif args.mode == "unlimited":
        scraper = SciScraperEnhanced(headless=True, max_pages=args.max_pages, scrape_type=scrape_type)
        run_mode = "default"
    elif args.mode == "production":
        scraper = SciScraperEnhanced(headless=True, max_pages=args.max_pages, scrape_type=scrape_type)
        run_mode = "production"
    elif args.mode == "custom":
        try:
            pages_input = input("Nombre de pages à scraper (0 ou vide = illimité): ").strip()
            pages = None if pages_input in ("", "0") else int(pages_input)
            headless = input("Mode headless (o/n): ").lower().startswith("o")
            scraper = SciScraperEnhanced(headless=headless, max_pages=args.max_pages or pages, scrape_type=scrape_type)
        except Exception:
            print("Configuration invalide, utilisation des paramètres par défaut")
            scraper = SciScraperEnhanced(headless=True, max_pages=args.max_pages, scrape_type=scrape_type)
        run_mode = "default"
    else:
        scraper = SciScraperEnhanced(headless=True, max_pages=args.max_pages, scrape_type=scrape_type)
        run_mode = "default"

    await scraper.run(mode=run_mode, send_email=args.send_email, email_to=args.email_to)


if __name__ == "__main__":
    # Installer beautifulsoup4 si pas déjà fait
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("📦 Installation de beautifulsoup4...")
        import subprocess
        subprocess.run(["pip", "install", "beautifulsoup4"])

    asyncio.run(main())