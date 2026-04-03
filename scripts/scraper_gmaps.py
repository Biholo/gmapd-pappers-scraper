"""
Service de scraping multi-departements et multi-niches Google Maps
Envoie directement les leads avec email vers Brevo (bon compte / bonne liste)
Usage: python scrape_departments.py
"""
import sys
import os
import re
import json
import time
import logging
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

# Charger les variables d'environnement depuis la racine
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.supabase_client import SupabaseClient
from services.brevo_service import BrevoService
from services.gsheets_service import GoogleSheetsService
from services_metier.scraper import GoogleMapsScraper

# Configuration du logging
logs_dir = Path(__file__).parent.parent / "logs"
logs_dir.mkdir(parents=True, exist_ok=True)
log_file = logs_dir / "scraping_gmaps.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(str(log_file), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

GMAPS_CONFIG_PATH = Path(__file__).parent.parent / "config" / "gmaps_scraping_config.json"
DAILY_STATS_PATH = Path(__file__).parent.parent / "logs" / "daily_stats.json"

# Regex stricte pour valider un email
EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9](?:[a-zA-Z0-9._%+-]{0,63}[a-zA-Z0-9])?@"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9.-]{0,253}[a-zA-Z0-9])?\.[a-zA-Z]{2,63}$"
)

# Emails generiques a ignorer (pas des vrais contacts)
BLACKLIST_PATTERNS = [
    r"^(info|contact|hello|admin|support|noreply|no-reply|webmaster|postmaster|mailer-daemon)@",
    r"@(example\.com|test\.com|localhost)$",
    r"\.png$|\.jpg$|\.jpeg$|\.gif$|\.svg$|\.webp$",
]
BLACKLIST_RE = [re.compile(p, re.IGNORECASE) for p in BLACKLIST_PATTERNS]

def load_gmaps_config():
    """Charger la configuration Google Maps."""
    with open(GMAPS_CONFIG_PATH, 'r', encoding='utf-8-sig') as f:
        return json.load(f)

# Global settings
config = load_gmaps_config()
settings = config.get("settings", {})
MAX_SCROLLS = settings.get("MAX_SCROLLS", 50)
HEADLESS = settings.get("HEADLESS", True)
DELAY_BETWEEN_CITIES = settings.get("DELAY_BETWEEN_CITIES", 2)
DELAY_BETWEEN_NICHES = settings.get("DELAY_BETWEEN_NICHES", 5)


def is_valid_email(email: str) -> bool:
    """Valider un email avec regex + blacklist."""
    if not email or not isinstance(email, str):
        return False
    email = email.strip().lower()
    if not EMAIL_REGEX.match(email):
        return False
    for pattern in BLACKLIST_RE:
        if pattern.search(email):
            return False
    return True


def save_gmaps_config(config):
    """Sauvegarder la configuration Google Maps."""
    with open(GMAPS_CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def mark_department_done(config, niche_index, dept_code):
    """Retirer un departement termine de la niche dans la configuration."""
    niches = config.get("niches", [])
    if niche_index < len(niches):
        depts = niches[niche_index].get("departments", [])
        if dept_code in depts:
            depts.remove(dept_code)
            save_gmaps_config(config)
            niche_name = niches[niche_index]["name"]
            logger.info(f"Departement {dept_code} retire de la config pour {niche_name} ({len(depts)} restants)")


def get_brevo_service_for_niche(niche_config):
    """Retourner un BrevoService avec la bonne API key pour une niche."""
    api_key_env = niche_config.get("brevo_api_key_env", "")
    api_key = os.getenv(api_key_env)
    if not api_key:
        logger.warning(f"API key {api_key_env} non configuree, envoi Brevo desactive pour {niche_config['name']}")
        return None
    return BrevoService(api_key=api_key)


def get_brevo_list_ids(niche_config):
    """Retourner les IDs de listes Brevo pour une niche (supporte liste ou string)."""
    raw = niche_config.get("brevo_list_id_env", [])
    if isinstance(raw, str):
        raw = [raw]
    ids = []
    for env_var in raw:
        val = os.getenv(env_var)
        if val:
            ids.append(int(val))
    return ids


# ── Daily stats ─────────────────────────────────────────────
def load_daily_stats() -> dict:
    """Charger les stats du jour."""
    DAILY_STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DAILY_STATS_PATH.exists():
        try:
            with open(DAILY_STATS_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if data.get("date") == date.today().isoformat():
                return data
        except Exception:
            pass
    return {"date": date.today().isoformat(), "gmaps_scraped": 0, "gmaps_brevo_sent": 0, "gmaps_brevo_dupes": 0}


def save_daily_stats(stats: dict):
    DAILY_STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DAILY_STATS_PATH, 'w', encoding='utf-8') as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


# ── Dedup set (emails deja envoyes cette session) ──────────
_sent_emails: set = set()


def format_phone_fr(phone: str) -> str:
    """Formate un numero francais en +33 pour Brevo SMS/Landline."""
    if not phone:
        return ""
    p = phone.replace(" ", "").replace(".", "").replace("-", "")
    if p.startswith("0"):
        p = "+33" + p[1:]
    return p


def process_lead(lead_data, email, socials, city_name, city_id, brevo_svc, list_ids, gsheets_svc, niche_name, stats, department, gsheets_rule="no_website"):
    """Gérer l'envoi vers Brevo (avec email) OU vers Google Sheets selon la règle de la niche.
    
    gsheets_rule:
    - "no_website": envoyer à Google Sheets si pas de site web (défaut)
    - "no_email": envoyer à Google Sheets si pas d'email
    """
    phone = lead_data.get("phone")
    website = lead_data.get("website")

    # Flag pour savoir si on a traité le lead d'une manière ou d'une autre
    processed = False

    # 1. Décider si on envoie à Google Sheets selon la règle
    should_send_gsheets = False
    if gsheets_rule == "no_email":
        # Pour CGP, Agent Immo, IAD: envoyer si pas d'email
        should_send_gsheets = phone and not email
    else:  # "no_website" (défaut)
        # Pour les autres: envoyer si pas de site web
        should_send_gsheets = phone and not website

    if should_send_gsheets:
        if gsheets_svc and gsheets_svc.send_to_gsheets(niche_name, lead_data, email, city_name, department, _sent_emails):
            processed = True

    # 2. Si on a un email (qu'il y ait un site ou non, la logique initiale est conservée pour Brevo)
    # Indépendant de Google Sheets
    if email:
        # Pour Brevo, la déduplication en session est toujours nécessaire (gérée dans send_to_brevo)
        if brevo_svc.send_to_brevo(list_ids, lead_data, email, socials, city_id, stats, _sent_emails, is_valid_email, format_phone_fr):
            save_daily_stats(stats)
            processed = True

    return processed

def main():
    config = load_gmaps_config()
    niches_config = config.get("niches", [])

    if not niches_config:
        logger.error("Aucune niche configuree. Arret.")
        return

    stats = load_daily_stats()

    logger.info("=" * 80)
    logger.info("DEMARRAGE DU SCRAPING GOOGLE MAPS")
    logger.info(f"Niches: {', '.join(n['name'] for n in niches_config)}")
    logger.info(f"Mode: {'HEADLESS' if HEADLESS else 'VISIBLE'}")
    logger.info("=" * 80)

    # Limite de temps: 2h30 max (9000 secondes)
    start_time = time.time()
    MAX_SESSION_DURATION = 2.5 * 3600

    # Initialiser Supabase
    try:
        supabase = SupabaseClient()
        logger.info("Client Supabase initialise")
    except Exception as e:
        logger.error(f"Erreur Supabase: {e}")
        return

    # Pre-initialiser les services Brevo et GSheets par niche
    brevo_services = {}
    brevo_list_ids_map = {}

    logger.info("Initialisation du service Google Sheets...")
    gsheets_service = GoogleSheetsService()

    if not getattr(gsheets_service, "client", None) or not getattr(gsheets_service, "main_spreadsheet", None):
        logger.warning("Google Sheets n'a pas pu être initialisé. Les leads sans site ne seront pas envoyés au Google Sheet.")
        logger.warning("Vérifiez que config/google_credentials.json existe et que GOOGLE_SHEETS_MASTER_SPREADSHEET_ID est configuré.")
        gsheets_service = None

    for niche_cfg in niches_config:
        name = niche_cfg["name"]
        brevo_services[name] = get_brevo_service_for_niche(niche_cfg)
        brevo_list_ids_map[name] = get_brevo_list_ids(niche_cfg)
        if brevo_services[name] and brevo_list_ids_map[name]:
            logger.info(f"  Brevo OK pour {name} -> listes {brevo_list_ids_map[name]} ({niche_cfg['brevo_api_key_env']})")
        else:
            logger.warning(f"  Brevo NON CONFIGURE pour {name}")

        if gsheets_service and gsheets_service.main_spreadsheet:
            logger.info(f"  GSheets OK pour {name} -> spreadsheet principal configuré")
            gsheets_service.ensure_sheet_exists(name)
        else:
            logger.warning(f"  GSheets NON CONFIGURE pour {name}")

    # Initialiser le scraper
    scraper = GoogleMapsScraper(headless=HEADLESS, max_scrolls=MAX_SCROLLS)

    total_scrapes = 0
    successful_scrapes = 0
    failed_scrapes = 0

    try:
        # Boucle principale: tourne tant qu'il reste au moins un département dans une niche
        while True:
            # Recharger la config à chaque tour pour avoir les dernières données
            config = load_gmaps_config()
            niches_config = config.get("niches", [])
            
            # Vérifier s'il reste du travail dans au moins une niche
            work_remaining = False
            for niche_cfg in niches_config:
                if niche_cfg.get("departments", []):
                    work_remaining = True
                    break
                    
            if not work_remaining:
                logger.info("Tous les départements de toutes les niches ont été traités.")
                break
                
            # Parcourir chaque niche pour traiter 1 seul département
            for niche_idx, niche_cfg in enumerate(niches_config):
                niche_name = niche_cfg["name"]
                search_query = niche_cfg["search_query"]
                remaining_depts = niche_cfg.get("departments", [])

                if not remaining_depts:
                    continue

                logger.info(f"\n{'=' * 80}")
                logger.info(f"NICHE: {niche_name} (query: {search_query})")
                logger.info(f"Departements restants: {len(remaining_depts)}")
                logger.info("=" * 80)

                brevo_svc = brevo_services.get(niche_name)
                list_ids = brevo_list_ids_map.get(niche_name, [])

                # Prendre uniquement le PREMIER département de la liste
                dept_code = remaining_depts[0]
                logger.info(f"\n--- Departement {dept_code} / {niche_name} ---")

                # Recuperer les villes du departement
                try:
                    cities = supabase.get_cities_by_department(dept_code)
                    logger.info(f"  {len(cities)} villes trouvees")
                except Exception as e:
                    logger.error(f"  Erreur chargement villes dept {dept_code}: {e}")
                    # En cas d'erreur, on passe à la niche suivante mais on ne marque pas le dept comme fait
                    continue

                if not cities:
                    logger.info("  Aucune ville, on marque comme termine")
                    mark_department_done(config, niche_idx, dept_code)
                    continue

                for city in cities:
                    if time.time() - start_time > MAX_SESSION_DURATION:
                        logger.info("Limite de temps (2h30) atteinte. Arret de la session.")
                        return

                    city_name = city['name']
                    city_id = city['id']

                    if scraper.is_already_scraped(city_id, search_query):
                        continue

                    logger.info(f"  [{city_name}] Scraping...")

                    try:
                        gsheets_rule = niche_cfg.get("gsheets_rule", "no_website")
                        def handle_lead(lead_data, email, socials, niche=niche_name, city=city_name, cid=city_id, brevo=brevo_svc, lists=list_ids, dept=dept_code, rule=gsheets_rule):
                            return process_lead(
                                lead_data, email, socials, city, cid,
                                brevo, lists, gsheets_service,
                                niche, stats, dept, rule
                            )

                        scraper.scrape(
                            city_name, city_id, search_query,
                            on_lead_enriched=handle_lead
                        )

                        total_scrapes += 1
                        stats["gmaps_scraped"] = stats.get("gmaps_scraped", 0) + 1
                        save_daily_stats(stats)
                        successful_scrapes += 1

                        time.sleep(DELAY_BETWEEN_CITIES)

                    except Exception as e:
                        failed_scrapes += 1
                        logger.error(f"  Erreur {city_name}: {e}")
                        continue

                # Departement termine -> retirer de la config
                mark_department_done(config, niche_idx, dept_code)
                logger.info(f"Département {dept_code} terminé pour la niche {niche_name}. Passage à la niche suivante...")
                time.sleep(DELAY_BETWEEN_NICHES)

    finally:
        scraper.close()

    logger.info(f"\n{'=' * 80}")
    logger.info("TERMINE")
    logger.info(f"Total: {total_scrapes} | Succes: {successful_scrapes} | Echecs: {failed_scrapes}")
    logger.info(f"Brevo: {stats.get('gmaps_brevo_sent', 0)} envoyes, {stats.get('gmaps_brevo_dupes', 0)} doublons")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
