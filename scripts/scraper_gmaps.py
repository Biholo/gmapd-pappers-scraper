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


def send_to_brevo(brevo_service, list_ids, lead_data, email, socials, city_id, stats):
    """Envoyer un lead vers Brevo si email valide et pas de doublon."""
    if not brevo_service or not list_ids or not email:
        return False

    email = email.strip().lower()

    if not is_valid_email(email):
        logger.debug(f"    Brevo: email invalide ignore: {email}")
        return False

    # Dedup local (meme session)
    if email in _sent_emails:
        logger.debug(f"    Brevo: doublon session ignore: {email}")
        stats["gmaps_brevo_dupes"] = stats.get("gmaps_brevo_dupes", 0) + 1
        save_daily_stats(stats)
        return False

    attributes = {}
    
    company = lead_data.get("name")
    if company:
        attributes["COMPANY"] = company
        attributes["NOM"] = company  # Maps company to NOM as fallback
        
    phone = lead_data.get("phone")
    if phone:
        attributes["TEL"] = phone
        formatted_phone = format_phone_fr(phone)
        if formatted_phone:
            attributes["SMS"] = formatted_phone
            attributes["LANDLINE_NUMBER"] = formatted_phone
            
    address = lead_data.get("address")
    if address:
        attributes["ADDRESS"] = address
        
    website = lead_data.get("website")
    if website:
        attributes["WEBSITE_URL"] = website
        
    if city_id:
        attributes["CITY_ID"] = str(city_id)
        
    if lead_data.get("numberOfRate") is not None:
        attributes["NUMBER_OF_RATE"] = lead_data.get("numberOfRate")
        
    if lead_data.get("averageRate") is not None:
        attributes["AVERAGE_RATE"] = lead_data.get("averageRate")
        
    attributes["SCRAPED_AT"] = date.today().strftime("%d/%m/%Y")
    
    if socials:
        if socials.get("instagramUrl"):
            attributes["INSTAGRAM_URL"] = socials.get("instagramUrl")
        if socials.get("facebookUrl"):
            attributes["FACEBOOK_URL"] = socials.get("facebookUrl")
        if socials.get("xUrl"):
            attributes["X_URL"] = socials.get("xUrl")

    try:
        brevo_service.create_contact(
            email=email,
            attributes=attributes,
            list_ids=list_ids,
            update_enabled=True,  # Brevo gere le conflit : met a jour si existe deja
        )
        _sent_emails.add(email)
        stats["gmaps_brevo_sent"] = stats.get("gmaps_brevo_sent", 0) + 1
        save_daily_stats(stats)
        logger.info(f"    Brevo: {email} ajoute aux listes {list_ids}")
        return True
    except Exception as e:
        err = str(e)
        if "duplicate" in err.lower() or "already" in err.lower() or "Contact already exist" in err:
            _sent_emails.add(email)
            stats["gmaps_brevo_dupes"] = stats.get("gmaps_brevo_dupes", 0) + 1
            save_daily_stats(stats)
            logger.info(f"    Brevo: {email} deja existant (doublon)")
            return False
        logger.warning(f"    Brevo: erreur ajout {email}: {e}")
        return False


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

    # Pre-initialiser les services Brevo par niche
    brevo_services = {}
    brevo_list_ids_map = {}
    for niche_cfg in niches_config:
        name = niche_cfg["name"]
        brevo_services[name] = get_brevo_service_for_niche(niche_cfg)
        brevo_list_ids_map[name] = get_brevo_list_ids(niche_cfg)
        if brevo_services[name] and brevo_list_ids_map[name]:
            logger.info(f"  Brevo OK pour {name} -> listes {brevo_list_ids_map[name]} ({niche_cfg['brevo_api_key_env']})")
        else:
            logger.warning(f"  Brevo NON CONFIGURE pour {name}")

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
                    logger.info(f"  Aucune ville, on marque comme termine")
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
                        scraper.scrape(
                            city_name, city_id, search_query,
                            on_lead_enriched=lambda lead_data, email, socials: (
                                send_to_brevo(brevo_svc, list_ids, lead_data, email, socials, city_id, stats) if email else False
                            )
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
