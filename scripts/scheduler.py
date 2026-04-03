#!/usr/bin/env python3
"""
Scheduler intelligent pour les scrapers.
- Lance gmaps en continu si aucun autre script ne tourne
- Les crons peuvent s'exécuter sans être bloqués par gmaps
- Vérifie l'état des processus avant de lancer gmaps
"""

import os
import sys
import time
import subprocess
import logging
from pathlib import Path
from datetime import datetime

# Setup path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/scheduler.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Processus à surveiller (ne pas lancer gmaps si l'un de ces scripts tourne)
BLOCKING_PROCESSES = [
    'scraper_pappers.py',  # SCI et Company
    'lead_transfer.py',     # Transfert GetSales -> Brevo
    'daily_recap.py'        # Recap quotidien
]

def is_process_running(script_name):
    """Vérifie si un script Python spécifique est en cours d'exécution."""
    try:
        result = subprocess.run(
            ['pgrep', '-f', script_name],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except Exception as e:
        logger.warning(f"Erreur lors de la vérification du processus {script_name}: {e}")
        return False

def any_blocking_process_running():
    """Vérifie si un processus bloquant est en cours d'exécution."""
    for process in BLOCKING_PROCESSES:
        if is_process_running(process):
            logger.info(f"Processus bloquant détecté: {process}")
            return True
    return False

def launch_gmaps():
    """Lance le scraper Google Maps en avant-plan avec logs visibles."""
    logger.info("Lancement du scraper Google Maps...")
    try:
        # Exécuter gmaps en avant-plan avec stdout/stderr redirigés vers le scheduler
        subprocess.run(
            ['python', '-u', 'scripts/scraper_gmaps.py'],
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=False
        )
        logger.info("Scraper Google Maps terminé")
        return True
    except Exception as e:
        logger.error(f"Erreur lors du lancement de gmaps: {e}")
        return False

def main():
    """Boucle principale du scheduler."""
    logger.info("=" * 80)
    logger.info("Démarrage du scheduler intelligent")
    logger.info("=" * 80)
    logger.info("Crons configurés:")
    logger.info(f"  SCI/Pappers      : {os.getenv('CRON_SCI', '15 10 * * *')}")
    logger.info(f"  Societes         : {os.getenv('CRON_COMPANY', '20 19 * * *')}")
    logger.info(f"  Transfert Brevo  : {os.getenv('CRON_TRANSFER', '30 14 * * *')}")
    logger.info(f"  Recap quotidien  : {os.getenv('CRON_RECAP', '0 23 * * *')}")
    logger.info("=" * 80)
    
    gmaps_running = False
    check_interval = 30  # Vérifier toutes les 30 secondes
    
    while True:
        try:
            blocking_running = any_blocking_process_running()
            
            if blocking_running:
                # Un processus bloquant tourne
                if gmaps_running:
                    logger.info("Processus bloquant détecté, arrêt de gmaps...")
                    gmaps_running = False
                time.sleep(check_interval)
                continue
            
            # Aucun processus bloquant ne tourne
            if not gmaps_running:
                # Vérifier si gmaps est vraiment arrêté
                if not is_process_running('scraper_gmaps.py'):
                    logger.info("Aucun processus bloquant en cours, lancement de gmaps...")
                    if launch_gmaps():
                        gmaps_running = True
                        time.sleep(5)  # Attendre un peu avant la prochaine vérification
                    else:
                        time.sleep(check_interval)
                else:
                    gmaps_running = True
            
            time.sleep(check_interval)
            
        except KeyboardInterrupt:
            logger.info("Arrêt du scheduler...")
            break
        except Exception as e:
            logger.error(f"Erreur dans la boucle principale: {e}")
            time.sleep(check_interval)

if __name__ == "__main__":
    main()
