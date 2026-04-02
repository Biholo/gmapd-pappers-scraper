#!/usr/bin/env python3
"""
Script de transfert de leads entre plateformes et listes.

3 fonctionnalites:
1. GetSales -> Brevo : transferer les leads enrichis (avec email ou tel) depuis GetSales vers Brevo
2. Brevo liste -> liste : transferer des leads d'une liste de stockage vers une liste workflow

Usage:
  python lead_transfer.py
"""

import json
import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple

# Setup path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

from services.brevo_service import BrevoService
from services.getsales_service import GetSalesService

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('lead_transfer.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# State file pour tracker les transferts quotidiens
TRANSFER_STATE_PATH = Path(__file__).parent.parent / "state" / "transfer_state.json"
DAILY_STATS_PATH = Path(__file__).parent.parent / "state" / "daily_stats.json"


# ============================================================
# Fonctions utilitaires
# ============================================================

def track_daily_stat(key: str, increment: int = 1):
    """Incrementer un compteur dans le fichier de stats quotidiennes."""
    try:
        DAILY_STATS_PATH.parent.mkdir(parents=True, exist_ok=True)
        stats = {}
        today = date.today().isoformat()
        if DAILY_STATS_PATH.exists():
            with open(DAILY_STATS_PATH, 'r', encoding='utf-8') as f:
                stats = json.load(f)
        if stats.get("date") != today:
            stats = {"date": today}
        stats[key] = stats.get(key, 0) + increment
        with open(DAILY_STATS_PATH, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def load_transfer_state() -> dict:
    """Charger l'etat des transferts (compteurs quotidiens)."""
    if TRANSFER_STATE_PATH.exists():
        try:
            with open(TRANSFER_STATE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_transfer_state(state: dict):
    """Sauvegarder l'etat des transferts."""
    TRANSFER_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TRANSFER_STATE_PATH, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_daily_transfer_count(state: dict, key: str) -> int:
    """Obtenir le nombre de transferts effectues aujourd'hui pour une cle donnee."""
    today = date.today().isoformat()
    entry = state.get(key, {})
    if entry.get("date") == today:
        return entry.get("count", 0)
    return 0


def increment_daily_transfer(state: dict, key: str, count: int = 1):
    """Incrementer le compteur de transferts quotidiens."""
    today = date.today().isoformat()
    entry = state.get(key, {})
    if entry.get("date") == today:
        entry["count"] = entry.get("count", 0) + count
    else:
        entry = {"date": today, "count": count}
    state[key] = entry
    save_transfer_state(state)


def get_search_dates(days_back: int = 1) -> Tuple[str, str]:
    """Retourne les dates from et to pour la recherche GetSales."""
    to_date = date.today()
    from_date = to_date - timedelta(days=days_back)
    return from_date.strftime("%Y-%m-%d"), to_date.strftime("%Y-%m-%d")


# ============================================================
# Logique Metier
# ============================================================

def transfer_getsales_to_brevo(
    getsales_list_uuid: str,
    brevo_api_key: str,
    brevo_list_logic: Dict[str, List[int]],  # "email": [list_ids], "phone": [list_ids]
    getsales_api_key: str = None,
    label: str = "",
    days_back: int = 1
):
    """Transferer les leads d'une liste GetSales vers des listes Brevo selon des regles.
    """
    gs_key = getsales_api_key or os.getenv("GETSALES_API_KEY")
    if not gs_key:
        logger.error("GETSALES_API_KEY non configuree")
        return 0

    from_date, to_date = get_search_dates(days_back)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"TRANSFERT GetSales -> Brevo [{label}]")
    logger.info(f"  GetSales list: {getsales_list_uuid}")
    logger.info(f"  Dates: {from_date} to {to_date}")
    logger.info(f"  Brevo logique: {brevo_list_logic}")
    logger.info(f"{'='*60}")

    gs_service = GetSalesService(api_key=gs_key)
    
    # Recuperer les leads GetSales recents
    leads = gs_service.search_leads(
        list_uuid=getsales_list_uuid,
        created_at_from=from_date,
        created_at_to=to_date,
        limit=2000
    )
    logger.info(f"  Total leads trouves dans GetSales pour ces dates: {len(leads)}")

    if not leads:
        logger.info("  Aucun lead a transferer")
        return 0

    # Initialiser Brevo
    brevo = BrevoService(api_key=brevo_api_key)

    transferred = 0
    skipped = 0

    for lead in leads:
        # Extraire l'email
        email = (
            lead.get("email")
            or lead.get("work_email")
            or lead.get("personal_email")
            or ""
        ).strip()
        
        has_email = bool(email and "@" in email)

        # Extraire le telephone
        phone = lead.get("phone") or lead.get("work_phone") or ""
        has_phone = bool(phone)
        
        # Determiner les listes cibles en fonction des donnees presentes
        target_lists = set()
        
        if has_email and "email" in brevo_list_logic:
            for l_id in brevo_list_logic["email"]:
                target_lists.add(l_id)
                
        if has_phone and "phone" in brevo_list_logic:
            for l_id in brevo_list_logic["phone"]:
                target_lists.add(l_id)
                
        if not target_lists:
            skipped += 1
            continue
            
        target_lists_list = list(target_lists)

        # On utilise l'email comme identifiant principal pour Brevo, 
        # mais si on n'a que le telephone, Brevo ne pourra pas creer le contact sans email.
        # Dans ce cas, si on veut vraiment creer un contact, il faudrait gerer autrement, 
        # mais Brevo requiert generalement l'email. Pour le moment on continue si pas d'email.
        if not has_email:
            # Si on veut gerer les envois sms-only, il faudrait un faux email ou config specifique.
            # Brevo permet la creation avec juste un telephone en general, mais l'API ici demande identifier=email.
            # Pour l'instant, on skip si pas d'email pour l'API BrevoService.
            skipped += 1
            continue

        # Construire les attributs Brevo
        attributes = {}
        first_name = lead.get("first_name", "")
        last_name = lead.get("last_name", "")
        if first_name:
            attributes["PRENOM"] = first_name
        if last_name:
            attributes["NOM"] = last_name
        company = lead.get("company_name") or lead.get("company", "")
        if company:
            attributes["COMPANY"] = company
        if phone:
            attributes["PHONE"] = phone
            # Formater en +33 si necessaire pour SMS
            p = phone.replace(" ", "").replace(".", "").replace("-", "")
            if p.startswith("0"):
                p = "+33" + p[1:]
            attributes["SMS"] = p
            attributes["TEL"] = phone
            
        linkedin = lead.get("linkedin_id") or lead.get("linkedin", "")
        if linkedin:
            if not linkedin.startswith("http"):
                linkedin = f"https://linkedin.com/in/{linkedin}"
            attributes["LINKEDIN"] = linkedin
        position = lead.get("position") or lead.get("headline") or ""
        if position:
            attributes["POSITION"] = position

        try:
            brevo.create_contact(
                email=email,
                attributes=attributes,
                list_ids=target_lists_list,
                update_enabled=True,
            )
            transferred += 1
            if transferred % 50 == 0:
                logger.info(f"  ... {transferred} leads transferes")
            time.sleep(0.1)  # Rate limiting
        except Exception as e:
            logger.warning(f"  Erreur Brevo pour {email}: {e}")

    if transferred > 0:
        track_daily_stat("transfer_getsales_brevo", transferred)

    logger.info(f"\nRESULTAT [{label}]:")
    logger.info(f"  Transferes: {transferred}")
    logger.info(f"  Ignores (pas d'info requise): {skipped}")
    return transferred


def transfer_brevo_list_to_list(
    brevo_api_key: str,
    from_list_id: int,
    to_list_id: int,
    max_per_day: int = 100,
    label: str = "",
):
    """Transferer des leads d'une liste Brevo vers une autre avec limite quotidienne."""
    state = load_transfer_state()
    state_key = f"brevo_{from_list_id}_to_{to_list_id}"
    already_done = get_daily_transfer_count(state, state_key)
    remaining = max(0, max_per_day - already_done)

    logger.info(f"\n{'='*60}")
    logger.info(f"TRANSFERT Brevo liste {from_list_id} -> {to_list_id} [{label}]")
    logger.info(f"  Max/jour: {max_per_day} | Deja fait aujourd'hui: {already_done} | Restant: {remaining}")
    logger.info(f"{'='*60}")

    if remaining <= 0:
        logger.info("  Limite quotidienne atteinte, rien a faire")
        return 0

    brevo = BrevoService(api_key=brevo_api_key)

    # Recuperer les contacts de la liste source
    transferred = 0
    offset = 0
    batch_size = 50

    while transferred < remaining:
        to_fetch = min(batch_size, remaining - transferred)
        try:
            result = brevo.get_contacts(
                limit=to_fetch,
                offset=offset,
                list_ids=[from_list_id],
            )
        except Exception as e:
            logger.error(f"  Erreur recuperation contacts: {e}")
            break

        contacts = result.get("contacts", []) if result else []
        if not contacts:
            logger.info("  Plus de contacts dans la liste source")
            break

        for contact in contacts:
            email = contact.get("email", "")
            if not email:
                continue

            try:
                # Ajouter a la liste destination
                brevo.update_contact(
                    identifier=email,
                    list_ids=[to_list_id],
                )

                # Retirer de la liste source
                brevo.update_contact(
                    identifier=email,
                    unlink_list_ids=[from_list_id],
                )

                transferred += 1
                time.sleep(0.1)

            except Exception as e:
                logger.warning(f"  Erreur transfert {email}: {e}")

        offset += len(contacts)

        if len(contacts) < to_fetch:
            break

    # Mettre a jour le compteur
    if transferred > 0:
        increment_daily_transfer(state, state_key, transferred)
        track_daily_stat("transfer_brevo_list", transferred)

    logger.info(f"\nRESULTAT [{label}]:")
    logger.info(f"  Transferes: {transferred}")
    logger.info(f"  Total aujourd'hui: {already_done + transferred}/{max_per_day}")
    return transferred


# ============================================================
# Main Execution
# ============================================================

def main():
    logger.info("Demarrage des operations de transfert")
    
    gs_key = os.getenv("GETSALES_API_KEY")
    opti_habitat_key = os.getenv("BREVO_OPTI_HABITAT_API_KEY")
    
    if not gs_key or not opti_habitat_key:
        logger.error("Cles API manquantes (GETSALES_API_KEY ou BREVO_OPTI_HABITAT_API_KEY)")
        return
        
    # Get IDs from Env
    brevo_sci_logiciel_list = int(os.getenv("BREVO_SCI_LOGICIEL_LIST", 9))
    brevo_sci_new_list = int(os.getenv("BREVO_SCI_NEW_LIST", 5))
    brevo_sci_old_list = int(os.getenv("BREVO_SCI_OLD_LIST", 6))
    
    # 1. Transfert GetSales -> Brevo
    # Règles : 
    # - Toutes les SCI -> BREVO_SCI_LOGICIEL_LIST si email
    # - Nouvelles SCI -> BREVO_SCI_NEW_LIST si telephone
    # - Anciennes SCI -> BREVO_SCI_OLD_LIST si email
    
    mappings = [
        (
            "GETSALES_NEW_SCI_LIST_UUID",
            opti_habitat_key,
            {
                "email": [brevo_sci_logiciel_list],
                "phone": [brevo_sci_new_list]
            },
            "Nouvelles SCI",
        ),
        (
            "GETSALES_OLD_SCI_LIST_UUID",
            opti_habitat_key,
            {
                "email": [brevo_sci_logiciel_list, brevo_sci_old_list],
                "phone": []
            },
            "Anciennes SCI",
        )
    ]

    total_gs_to_brevo = 0
    for gs_env, brevo_key, brevo_list_logic, label in mappings:
        gs_uuid = os.getenv(gs_env)
        if not gs_uuid:
            logger.warning(f"  {gs_env} non configure, skip")
            continue

        count = transfer_getsales_to_brevo(
            getsales_list_uuid=gs_uuid,
            brevo_api_key=brevo_key,
            brevo_list_logic=brevo_list_logic,
            label=label,
            days_back=2 # Recherche sur les 2 derniers jours pour ratisser un peu
        )
        total_gs_to_brevo += count

    # 2. Transfert Brevo Liste -> Liste
    brevo_transfers = [
        # Exemple : ("BREVO_STORAGE_LIST", "BREVO_WORKFLOW_LIST", 100, "BREVO_OPTI_HABITAT_API_KEY"),
    ]
    
    total_brevo_to_brevo = 0
    for from_env, to_env, max_day, api_env in brevo_transfers:
        from_id_str = os.getenv(from_env)
        to_id_str = os.getenv(to_env)
        api_key = os.getenv(api_env)
        
        if from_id_str and to_id_str and api_key:
            try:
                from_id = int(from_id_str)
                to_id = int(to_id_str)
                count = transfer_brevo_list_to_list(
                    brevo_api_key=api_key,
                    from_list_id=from_id,
                    to_list_id=to_id,
                    max_per_day=max_day,
                    label=f"{from_env} -> {to_env}"
                )
                total_brevo_to_brevo += count
            except ValueError:
                logger.error(f"List IDs invalides: {from_id_str} ou {to_id_str}")
                
    logger.info("="*60)
    logger.info("Rapport d'execution:")
    logger.info(f"Total GetSales -> Brevo: {total_gs_to_brevo}")
    logger.info(f"Total Brevo -> Brevo: {total_brevo_to_brevo}")
    logger.info("="*60)


if __name__ == "__main__":
    main()

