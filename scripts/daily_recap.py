#!/usr/bin/env python3
"""
Recapitulatif quotidien des operations de scraping.
Envoie un rapport par email via Resend avec les stats de la journee.

Usage:
  python daily_recap.py                # Affiche le recap dans la console
  python daily_recap.py --send-email   # Envoie le recap par email
"""

import argparse
import json
import os
import sys
import logging
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

from services.resend_service import ResendService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DAILY_STATS_PATH = Path(__file__).parent.parent / "logs" / "daily_stats.json"


def load_daily_stats() -> dict:
    """Charger les stats du jour."""
    if not DAILY_STATS_PATH.exists():
        return {"date": date.today().isoformat()}
    try:
        with open(DAILY_STATS_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if data.get("date") == date.today().isoformat():
            return data
        return {"date": date.today().isoformat()}
    except Exception:
        return {"date": date.today().isoformat()}


def build_recap_text(stats: dict) -> str:
    """Construire le texte du recap."""
    lines = []
    lines.append(f"RECAP QUOTIDIEN - {stats.get('date', date.today().isoformat())}")
    lines.append("=" * 50)
    lines.append("")

    # Google Maps
    lines.append("GOOGLE MAPS")
    lines.append(f"  Villes scrapees       : {stats.get('gmaps_scraped', 0)}")
    lines.append(f"  Leads envoyes Brevo   : {stats.get('gmaps_brevo_sent', 0)}")
    lines.append(f"  Doublons Brevo        : {stats.get('gmaps_brevo_dupes', 0)}")
    lines.append("")

    # Pappers
    lines.append("PAPPERS (SCI + SOCIETES)")
    lines.append(f"  Entreprises scrapees  : {stats.get('pappers_scraped', 0)}")
    lines.append(f"  Leads envoyes GetSales: {stats.get('pappers_getsales_sent', 0)}")
    lines.append(f"  Enrichissements files : {stats.get('pappers_enrichment_queued', 0)}")
    lines.append("")

    # Transferts
    lines.append("TRANSFERTS")
    lines.append(f"  GetSales -> Brevo     : {stats.get('transfer_getsales_brevo', 0)}")
    lines.append(f"  Brevo liste -> liste  : {stats.get('transfer_brevo_list', 0)}")
    lines.append("")

    # Total
    total_scraped = stats.get('gmaps_scraped', 0) + stats.get('pappers_scraped', 0)
    total_brevo = stats.get('gmaps_brevo_sent', 0) + stats.get('transfer_getsales_brevo', 0) + stats.get('transfer_brevo_list', 0)
    lines.append("TOTAUX")
    lines.append(f"  Total scrapees        : {total_scraped}")
    lines.append(f"  Total envoyes Brevo   : {total_brevo}")
    lines.append(f"  Total GetSales        : {stats.get('pappers_getsales_sent', 0)}")
    lines.append("=" * 50)

    return "\n".join(lines)


def build_recap_html(stats: dict) -> str:
    """Construire le HTML du recap pour l'email."""
    d = stats.get('date', date.today().isoformat())

    gmaps_scraped = stats.get('gmaps_scraped', 0)
    gmaps_brevo = stats.get('gmaps_brevo_sent', 0)
    gmaps_dupes = stats.get('gmaps_brevo_dupes', 0)
    pappers_scraped = stats.get('pappers_scraped', 0)
    pappers_gs = stats.get('pappers_getsales_sent', 0)
    pappers_enrich = stats.get('pappers_enrichment_queued', 0)
    transfer_gs_brevo = stats.get('transfer_getsales_brevo', 0)
    transfer_brevo = stats.get('transfer_brevo_list', 0)

    total_scraped = gmaps_scraped + pappers_scraped
    total_brevo = gmaps_brevo + transfer_gs_brevo + transfer_brevo

    return f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
        <h2 style="color: #2c3e50;">Recap Scraping - {d}</h2>

        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
            <tr style="background: #3498db; color: white;">
                <th colspan="2" style="padding: 10px; text-align: left;">Google Maps</th>
            </tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">Villes scrapees</td><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">{gmaps_scraped}</td></tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">Leads envoyes Brevo</td><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold; color: #27ae60;">{gmaps_brevo}</td></tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">Doublons</td><td style="padding: 8px; border-bottom: 1px solid #eee; color: #e67e22;">{gmaps_dupes}</td></tr>
        </table>

        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
            <tr style="background: #9b59b6; color: white;">
                <th colspan="2" style="padding: 10px; text-align: left;">Pappers (SCI + Societes)</th>
            </tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">Entreprises scrapees</td><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">{pappers_scraped}</td></tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">Leads envoyes GetSales</td><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold; color: #27ae60;">{pappers_gs}</td></tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">Enrichissements files</td><td style="padding: 8px; border-bottom: 1px solid #eee;">{pappers_enrich}</td></tr>
        </table>

        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
            <tr style="background: #e74c3c; color: white;">
                <th colspan="2" style="padding: 10px; text-align: left;">Transferts</th>
            </tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">GetSales -> Brevo</td><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">{transfer_gs_brevo}</td></tr>
            <tr><td style="padding: 8px; border-bottom: 1px solid #eee;">Brevo liste -> liste</td><td style="padding: 8px; border-bottom: 1px solid #eee; font-weight: bold;">{transfer_brevo}</td></tr>
        </table>

        <div style="background: #2c3e50; color: white; padding: 15px; border-radius: 5px;">
            <h3 style="margin-top: 0;">Totaux</h3>
            <p>Total scrapees : <strong>{total_scraped}</strong></p>
            <p>Total envoyes Brevo : <strong style="color: #2ecc71;">{total_brevo}</strong></p>
            <p>Total GetSales : <strong>{pappers_gs}</strong></p>
        </div>
    </body>
    </html>
    """


def send_recap_email(stats: dict, recipients: list):
    """Envoyer le recap par email via Resend."""
    try:
        resend_svc = ResendService()
        d = stats.get('date', date.today().isoformat())
        html = build_recap_html(stats)

        resend_svc.send_report(
            to=recipients,
            subject=f"Recap Scraping {d}",
            html_body=html,
        )
        logger.info(f"Recap envoye a : {', '.join(recipients)}")
    except Exception as e:
        logger.error(f"Erreur envoi recap: {e}")


def main():
    parser = argparse.ArgumentParser(description="Recap quotidien du scraping")
    parser.add_argument("--send-email", action="store_true", help="Envoyer le recap par email")
    parser.add_argument("--email-to", nargs="+", help="Destinataires (defaut: RESEND_TO_EMAIL)")
    args = parser.parse_args()

    stats = load_daily_stats()
    recap_text = build_recap_text(stats)
    print(recap_text)

    if args.send_email:
        recipients = args.email_to or ["kilian@develly.io"]
        recipients = [r for r in recipients if r]
        if recipients:
            send_recap_email(stats, recipients)
        else:
            logger.warning("Pas de destinataire configure")


if __name__ == "__main__":
    main()
