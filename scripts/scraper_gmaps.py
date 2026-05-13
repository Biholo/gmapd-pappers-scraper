"""
Service de scraping multi-niches Google Maps
Missions lues depuis scrape_progress (Supabase), leads pushés vers Brevo (compte unique).
Usage: python scraper_gmaps.py
"""
import sys
import os
import re
import json
import time
import random
import logging
from collections import defaultdict
from pathlib import Path
from datetime import date
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

sys.path.insert(0, str(Path(__file__).parent.parent))

from services.supabase_client import SupabaseClient  # noqa: E402
from services.brevo_service import BrevoService  # noqa: E402
from services.gsheets_service import GoogleSheetsService  # noqa: E402
from services_metier.scraper import GoogleMapsScraper  # noqa: E402

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

DAILY_STATS_PATH = Path(__file__).parent.parent / "logs" / "daily_stats.json"

EMAIL_REGEX = re.compile(
    r"^[a-zA-Z0-9](?:[a-zA-Z0-9._%+-]{0,63}[a-zA-Z0-9])?@"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9.-]{0,253}[a-zA-Z0-9])?\.[a-zA-Z]{2,63}$"
)

BLACKLIST_PATTERNS = [
    r"^(info|contact|hello|admin|support|noreply|no-reply|webmaster|postmaster|mailer-daemon)@",
    r"@(example\.com|test\.com|localhost)$",
    r"\.png$|\.jpg$|\.jpeg$|\.gif$|\.svg$|\.webp$",
]
BLACKLIST_RE = [re.compile(p, re.IGNORECASE) for p in BLACKLIST_PATTERNS]


def _getenv_bool(name, default):
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


MAX_SCROLLS = int(os.getenv("GMAPS_MAX_SCROLLS", "50"))
HEADLESS = _getenv_bool("GMAPS_HEADLESS", True)
DELAY_BETWEEN_CITIES = int(os.getenv("DELAY_BETWEEN_CITIES", "2"))
DELAY_BETWEEN_NICHES = int(os.getenv("DELAY_BETWEEN_NICHES", "5"))


def is_valid_email(email: str) -> bool:
    if not email or not isinstance(email, str):
        return False
    email = email.strip().lower()
    if not EMAIL_REGEX.match(email):
        return False
    for pattern in BLACKLIST_RE:
        if pattern.search(email):
            return False
    return True


def load_daily_stats() -> dict:
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


_sent_emails: set = set()
_sent_phones: set = set()


def format_phone_fr(phone: str) -> str:
    if not phone:
        return ""
    p = phone.replace(" ", "").replace(".", "").replace("-", "")
    if p.startswith("0"):
        p = "+33" + p[1:]
    return p


def _env_int(name: str) -> int:
    val = os.getenv(name, "").strip()
    return int(val) if val.isdigit() else 0


def _get_proprietaire(niche: str) -> str:
    n = (niche or "").lower()
    if n in ("iad", "agent immo"):
        return "Immo Trust"
    if n == "cgp":
        return "Rentium"
    return "Develly"


def process_lead(lead_data, email, socials, city_name, city_id, brevo_svc, gsheets_svc, niche_name, stats, department):
    """Routing:
    - GSheets  → lead has phone AND no WhatsApp
    - Brevo email list  → lead has email
    - Brevo WA list     → lead has WhatsApp
    - Brevo SMS list    → lead has phone but no WhatsApp
    """
    lead_data["proprietaire"] = _get_proprietaire(niche_name)
    phone = lead_data.get("phone")
    has_whatsapp = lead_data.get("has_whatsapp", False)
    processed = False

    # Google Sheets: only if no WhatsApp
    if phone and not has_whatsapp and gsheets_svc:
        if gsheets_svc.send_to_gsheets(niche_name, lead_data, email, city_name, department, _sent_emails):
            processed = True

    if not brevo_svc:
        return processed

    email_list_id = _env_int("BREVO_LIST_EMAIL_ID")
    wa_list_id = _env_int("BREVO_LIST_WA_ID")
    sms_list_id = _env_int("BREVO_LIST_SMS_ID")

    # Build target lists for this lead (can be in multiple)
    target_lists = []
    if email and email_list_id:
        target_lists.append(email_list_id)
    if has_whatsapp and wa_list_id:
        target_lists.append(wa_list_id)
    elif phone and not has_whatsapp and sms_list_id:
        target_lists.append(sms_list_id)

    if not target_lists:
        return processed

    if email:
        # Email contact: use email as Brevo identifier, include all target lists at once
        if brevo_svc.send_to_brevo(target_lists, lead_data, email, socials, city_id, stats, _sent_emails, is_valid_email, format_phone_fr):
            save_daily_stats(stats)
            processed = True
    elif phone:
        # Phone-only contact: use formatted phone as ext_id
        formatted = format_phone_fr(phone)
        if brevo_svc.send_phone_to_brevo(target_lists, lead_data, formatted, city_id, stats, _sent_phones):
            save_daily_stats(stats)
            processed = True

    return processed


def main():
    stats = load_daily_stats()

    logger.info("=" * 80)
    logger.info("DEMARRAGE DU SCRAPING GOOGLE MAPS")
    logger.info(f"Mode: {'HEADLESS' if HEADLESS else 'VISIBLE'}")
    logger.info("=" * 80)

    start_time = time.time()
    max_session_duration = 2.5 * 3600

    try:
        supabase = SupabaseClient()
        logger.info("Client Supabase initialise")
    except Exception as e:
        logger.error(f"Erreur Supabase: {e}")
        return

    brevo_api_key = os.getenv("BREVO_API_KEY")
    if brevo_api_key:
        brevo_svc = BrevoService(api_key=brevo_api_key)
        logger.info(
            f"Brevo OK -> email={_env_int('BREVO_LIST_EMAIL_ID')} "
            f"wa={_env_int('BREVO_LIST_WA_ID')} "
            f"sms={_env_int('BREVO_LIST_SMS_ID')}"
        )
    else:
        brevo_svc = None
        logger.warning("BREVO_API_KEY non configuree, envoi Brevo desactive")

    logger.info("Initialisation du service Google Sheets...")
    gsheets_service = GoogleSheetsService()
    if not getattr(gsheets_service, "client", None) or not getattr(gsheets_service, "main_spreadsheet", None):
        logger.warning("Google Sheets non configure.")
        gsheets_service = None

    scraper = GoogleMapsScraper(headless=HEADLESS, max_scrolls=MAX_SCROLLS)

    total_scrapes = 0
    successful_scrapes = 0
    failed_scrapes = 0

    try:
        while True:
            if time.time() - start_time > max_session_duration:
                logger.info("Limite de temps (2h30) atteinte. Arret.")
                break

            missions = supabase.get_pending_missions()
            if not missions:
                logger.info("Aucune mission pending. Arret.")
                break

            # Build batch: one random city per niche, shuffled — guarantees niche diversity
            niche_groups: defaultdict = defaultdict(list)
            for m in missions:
                niche_groups[m["niche"]].append(m)
            batch = [random.choice(cities) for cities in niche_groups.values()]
            random.shuffle(batch)
            logger.info(f"Batch: {len(batch)} niches — {' | '.join(m['niche'] for m in batch)}")

            for mission in batch:
                if time.time() - start_time > max_session_duration:
                    logger.info("Limite de temps (2h30) atteinte. Arret.")
                    break

                niche_name = mission["niche"]
                city_id = mission["city_id"]
                city_name = mission["name"]
                dept_code = mission.get("department_code", "")

                if scraper.is_already_scraped(city_id, niche_name):
                    supabase.update_scrape_progress(niche_name, city_id, "done", leads_found=0)
                    continue

                logger.info(f"\n{'=' * 80}")
                logger.info(f"MISSION: {niche_name} / {city_name} (dept {dept_code})")
                logger.info("=" * 80)

                city_leads_sent = 0

                def handle_lead(lead_data, email, socials,
                                _niche=niche_name, _city=city_name, _cid=city_id,
                                _brevo=brevo_svc, _dept=dept_code):
                    nonlocal city_leads_sent
                    result = process_lead(
                        lead_data, email, socials, _city, _cid,
                        _brevo, gsheets_service,
                        _niche, stats, _dept
                    )
                    if result:
                        city_leads_sent += 1
                    return result

                try:
                    success = scraper.scrape(city_name, city_id, niche_name, on_lead_enriched=handle_lead)
                    if success:
                        supabase.update_scrape_progress(niche_name, city_id, "done", leads_found=city_leads_sent)
                        stats["gmaps_scraped"] = stats.get("gmaps_scraped", 0) + 1
                        save_daily_stats(stats)
                        successful_scrapes += 1
                    else:
                        supabase.update_scrape_progress(niche_name, city_id, "error", error_msg="Chrome crash during scroll")
                        failed_scrapes += 1
                    total_scrapes += 1
                    time.sleep(DELAY_BETWEEN_CITIES)
                except Exception as e:
                    failed_scrapes += 1
                    logger.error(f"Erreur {city_name}/{niche_name}: {e}")
                    supabase.update_scrape_progress(niche_name, city_id, "error", error_msg=str(e))

    finally:
        scraper.close()

    logger.info(f"\n{'=' * 80}")
    logger.info("TERMINE")
    logger.info(f"Total: {total_scrapes} | Succes: {successful_scrapes} | Echecs: {failed_scrapes}")
    logger.info(f"Brevo: {stats.get('gmaps_brevo_sent', 0)} envoyes, {stats.get('gmaps_brevo_dupes', 0)} doublons")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
