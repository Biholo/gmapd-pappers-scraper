# ============================================
# Dockerfile - Scraping Lead
# Image unifiée : Selenium (Chrome) + Playwright (Chromium)
# ============================================

# --- Stage 1 : Builder (installation des dépendances Python) ---
FROM python:3.12-slim AS builder

WORKDIR /build

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# --- Stage 2 : Runtime ---
FROM python:3.12-slim AS runtime

# Métadonnées
LABEL maintainer="kilian@develly.io"
LABEL description="Scrapers de leads B2B (Google Maps + Pappers/SCI)"

# Variables d'environnement par défaut
# Python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    HEADLESS=True \
    LOG_LEVEL=INFO \
    DISPLAY=:99 \
    DOCKER=true
# Selenium : utiliser Chromium du système
ENV CHROME_BIN=/usr/bin/chromium \
    CHROMEDRIVER_PATH=/usr/bin/chromedriver

# Dépendances système : Chrome + Chromium + outils
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Chrome/Chromium pour Selenium
    chromium \
    chromium-driver \
    # Dépendances Playwright
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    libwayland-client0 \
    # Xvfb pour le rendu virtuel
    xvfb \
    # Cron pour le scheduling
    cron \
    # Divers
    fonts-liberation \
    dumb-init \
    && rm -rf /var/lib/apt/lists/*

# Copier les packages Python depuis le builder
COPY --from=builder /install /usr/local

# Installer les navigateurs Playwright
RUN playwright install chromium

# Créer un user non-root pour la sécurité
RUN groupadd -r scraper && useradd -r -g scraper -d /app -s /sbin/nologin scraper

WORKDIR /app

# Copier le code source
COPY config/ config/
COPY scripts/ scripts/
COPY services/ services/
COPY services_metier/ services_metier/

# Créer les répertoires pour les volumes
RUN mkdir -p /app/csv /app/logs /app/state \
    && chown -R scraper:scraper /app

# Copier l'entrypoint
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Healthcheck basique (vérifie que Python fonctionne)
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "import requests; print('ok')" || exit 1

# dumb-init gère proprement les signaux (PID 1)
ENTRYPOINT ["dumb-init", "--", "/entrypoint.sh"]

# Commande par défaut : affiche l'aide
CMD ["help"]
