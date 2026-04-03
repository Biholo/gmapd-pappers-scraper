#!/bin/bash
set -e

# ============================================
# Entrypoint unifié pour les scrapers
# Usage :
#   docker run scraping-lead gmaps        → lance le scraper Google Maps
#   docker run scraping-lead sci          → lance le scraper SCI/Pappers
#   docker run scraping-lead company      → lance le scraper societes (SASU/SAS/EURL/SARL)
#   docker run scraping-lead transfer     → lance le transfert GetSales -> Brevo
#   docker run scraping-lead cron         → lance le scheduler cron
#   docker run scraping-lead help         → affiche l'aide
# ============================================

# Démarrer Xvfb en arrière-plan (écran virtuel pour les navigateurs)
Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp &
export DISPLAY=:99

# Se placer dans le répertoire de l'app
cd /app

# Lier les fichiers de state depuis le volume
link_state() {
    for f in .scraped_history.json enrichment_yahoo_state.json; do
        if [ -f "/app/logs/$f" ]; then
            ln -sf "/app/logs/$f" "/app/$f"
        else
            touch "/app/logs/$f"
            ln -sf "/app/logs/$f" "/app/$f"
        fi
    done
    # Config JSON persistants
    for f in scraper_config.json scraper_config_company.json; do
        if [ -f "/app/config/$f" ]; then
            ln -sf "/app/config/$f" "/app/$f"
        fi
    done
}

case "${1:-help}" in
    gmaps)
        echo "[$(date)] Lancement du scraper Google Maps..."
        link_state
        exec python -u scripts/scraper_gmaps.py 2>&1 | tee /app/logs/gmaps_$(date +%Y%m%d_%H%M%S).log
        ;;

    sci)
        echo "[$(date)] Lancement du scraper SCI/Pappers (mode production)..."
        link_state
        exec python -u scripts/scraper_pappers.py --mode production --type sci --send-email 2>&1 | tee /app/logs/sci_$(date +%Y%m%d_%H%M%S).log
        ;;

    company)
        echo "[$(date)] Lancement du scraper Societes SASU/SAS/EURL/SARL (mode production)..."
        link_state
        exec python -u scripts/scraper_pappers.py --mode production --type company --send-email 2>&1 | tee /app/logs/company_$(date +%Y%m%d_%H%M%S).log
        ;;

    transfer)
        echo "[$(date)] Lancement du transfert GetSales -> Brevo..."
        link_state
        exec python -u scripts/lead_transfer.py getsales-to-brevo 2>&1 | tee /app/logs/transfer_$(date +%Y%m%d_%H%M%S).log
        ;;

    brevo-transfer)
        echo "[$(date)] Lancement du transfert Brevo liste -> liste..."
        link_state
        shift
        exec python -u scripts/lead_transfer.py brevo-transfer "$@" 2>&1 | tee /app/logs/brevo_transfer_$(date +%Y%m%d_%H%M%S).log
        ;;

    recap)
        echo "[$(date)] Lancement du recap quotidien..."
        link_state
        shift
        exec python -u scripts/daily_recap.py "$@" 2>&1 | tee /app/logs/recap_$(date +%Y%m%d_%H%M%S).log
        ;;

    cron)
        echo "[$(date)] Démarrage du scheduler intelligent..."
        link_state

        # Générer le crontab depuis les variables d'environnement
        CRON_SCI="${CRON_SCI:-15 10 * * *}"
        CRON_COMPANY="${CRON_COMPANY:-20 19 * * *}"
        CRON_TRANSFER="${CRON_TRANSFER:-30 14 * * *}"
        CRON_RECAP="${CRON_RECAP:-0 23 * * *}"

        # Exporter toutes les variables d'env pour cron
        printenv | grep -v "no_proxy" >> /etc/environment

        cat > /etc/cron.d/scrapers <<EOF
SHELL=/bin/bash
DISPLAY=:99

# Scraper SCI
${CRON_SCI} root cd /app && python -u scripts/scraper_pappers.py --mode production --type sci --send-email >> /app/logs/sci_cron.log 2>&1

# Scraper Societes (SASU/SAS/EURL/SARL)
${CRON_COMPANY} root cd /app && python -u scripts/scraper_pappers.py --mode production --type company --send-email >> /app/logs/company_cron.log 2>&1

# Transfert GetSales -> Brevo
${CRON_TRANSFER} root cd /app && python -u scripts/lead_transfer.py getsales-to-brevo >> /app/logs/transfer_cron.log 2>&1

# Recap quotidien (22h)
${CRON_RECAP} root cd /app && python -u scripts/daily_recap.py --send-email >> /app/logs/recap_cron.log 2>&1
EOF

        chmod 0644 /etc/cron.d/scrapers
        crontab /etc/cron.d/scrapers

        echo "Cron configuré :"
        echo "  SCI/Pappers      : ${CRON_SCI}"
        echo "  Societes         : ${CRON_COMPANY}"
        echo "  Transfert Brevo  : ${CRON_TRANSFER}"
        echo "  Recap quotidien  : ${CRON_RECAP}"
        echo "Logs dans /app/logs/"

        # Lancer cron en arrière-plan (pour les crons)
        cron &
        CRON_PID=$!
        
        # Lancer le scheduler Python intelligent en avant-plan
        exec python -u scripts/scheduler.py
        ;;

    shell|bash)
        exec /bin/bash
        ;;

    help|*)
        echo "=== Scraping Lead - Docker ==="
        echo ""
        echo "Usage: docker run scraping-lead <commande>"
        echo ""
        echo "Commandes disponibles :"
        echo "  gmaps          Lancer le scraper Google Maps (départements)"
        echo "  sci            Lancer le scraper SCI/Pappers avec enrichissement"
        echo "  company        Lancer le scraper Sociétés (SASU/SAS/EURL/SARL)"
        echo "  transfer       Lancer le transfert GetSales -> Brevo"
        echo "  brevo-transfer Transfert liste -> liste Brevo (ex: brevo-transfer --from-list 5 --to-list 12)"
        echo "  cron           Démarrer le scheduler (exécution planifiée)"
        echo "  shell          Ouvrir un shell interactif"
        echo "  help           Afficher cette aide"
        echo ""
        echo "Variables d'environnement :"
        echo "  CRON_GMAPS      Cron schedule Google Maps (défaut: '0 6 * * *')"
        echo "  CRON_SCI        Cron schedule SCI (défaut: '0 8 * * *')"
        echo "  CRON_COMPANY    Cron schedule Sociétés (défaut: '0 10 * * *')"
        echo "  CRON_TRANSFER   Cron schedule transfert (défaut: '0 14 * * *')"
        echo "  LOG_LEVEL       Niveau de log (défaut: INFO)"
        echo ""
        ;;
esac
