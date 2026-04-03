# Scraping Lead - Pipeline de prospection automatisée

Scraping Google Maps, Pappers (SCI + Sociétés), transfert GetSales/Brevo.

---

## 🚀 Démarrage rapide

### Installation locale

```bash
git clone <repo>
cd scraping-lead
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows
pip install -r requirements.txt
playwright install chromium
```

Copier `.env.example` en `.env` et remplir les credentials.

---

## 📦 Docker - Commandes simples

### Build
```bash
docker compose build
```

### Lancer les scrapers

```bash
# Google Maps (scrape toute la France par niche)
docker compose up gmaps

# SCI Pappers
docker compose run --rm sci

# Sociétés (SASU/SAS/EURL/SARL)
docker compose run --rm company

# Transfert GetSales -> Brevo
docker compose run --rm transfer

# Recap quotidien
docker compose run --rm scheduler recap --send-email
```

### Scheduler automatique (cron)

```bash
# Lance gmaps en continu + crons pour SCI/Company/Transfer/Recap
docker compose up scheduler
```

**Crons configurables** (dans `.env`) :
```env
CRON_SCI=15 10 * * *        # SCI à 10h15
CRON_COMPANY=20 19 * * *    # Sociétés à 19h20
CRON_TRANSFER=30 14 * * *   # Transfert à 14h30
CRON_RECAP=0 23 * * *       # Recap à 23h
```

---

## 📋 Scripts locaux

### Google Maps
```bash
python scripts/scraper_gmaps.py
```
- Scrape toute la France par niche
- Envoie directement vers Brevo si email trouvé
- Envoie vers Google Sheets si téléphone sans site web
- Config: `config/gmaps_scraping_config.json`

### Pappers (SCI / Sociétés)
```bash
# SCI
python scripts/scraper_pappers.py --type sci --mode production --send-email

# Sociétés
python scripts/scraper_pappers.py --type company --mode production --send-email

# Mode test (2 pages max)
python scripts/scraper_pappers.py --type sci --mode test

# Mode visible (navigateur affiché)
python scripts/scraper_pappers.py --type sci --mode visible
```

### Transfert de leads
```bash
# GetSales -> Brevo
python scripts/lead_transfer.py getsales-to-brevo

# Brevo liste -> liste (max 100/jour)
python scripts/lead_transfer.py brevo-transfer --from-list 5 --to-list 12 --max-per-day 100
```

### Recap quotidien
```bash
# Afficher dans la console
python scripts/daily_recap.py

# Envoyer par email
python scripts/daily_recap.py --send-email
```

---

## 🔧 Configuration

### Google Maps (`config/gmaps_scraping_config.json`)

Chaque niche contient :
- `name` : Nom de la niche
- `search_query` : Requête Google Maps
- `brevo_api_key_env` : Variable env de la clé API Brevo
- `brevo_list_id_env` : IDs des listes Brevo (array)
- `departments` : Départements à scraper (retirés automatiquement)

Paramètres globaux :
- `MAX_SCROLLS` : Nombre max de scrolls par ville (défaut: 50)
- `HEADLESS` : Mode headless (défaut: true)
- `DELAY_BETWEEN_CITIES` : Délai entre villes en secondes (défaut: 2)
- `DELAY_BETWEEN_NICHES` : Délai entre niches (défaut: 5)

---

## 📊 Flux de données

```
Google Maps  ──> Supabase + Brevo (si email) + Google Sheets (si téléphone sans site)
Pappers SCI  ──> GetSales ──> Brevo (enrichissement LinkedIn)
Pappers Co.  ──> GetSales ──> Brevo (enrichissement LinkedIn)
```

---

## 🔑 Variables d'environnement

```env
# Brevo (2 comptes)
BREVO_OPTI_HABITAT_API_KEY=xkeysib-...
BREVO_DEVELLY_API_KEY=xkeysib-...

# Listes Brevo (Opti Habitat)
BREVO_AGENT_IMMO_LIST=4
BREVO_SCI_NEW_LIST=5
BREVO_SCI_OLD_LIST=6

# Listes Brevo (Develly)
BREVO_SERRURIER_LIST=16
BREVO_PLUMBER_LIST=15
BREVO_ELECTRICIAN_LIST=14

# GetSales
GETSALES_BASE_URL=https://amazing.getsales.io
GETSALES_API_KEY=...
GETSALES_NEW_SCI_LIST_UUID=...
GETSALES_OLD_SCI_LIST_UUID=...
GETSALES_NEW_COMPANY_LIST_UUID=...

# Supabase
SUPABASE_URL=...
SUPABASE_KEY=...

# Google Sheets
GOOGLE_SHEETS_MASTER_SPREADSHEET_ID=...

# Resend (emails)
RESEND_API_KEY=...
RESEND_FROM_EMAIL=...
RESEND_TO_EMAIL=...
```

---

## 📁 Structure du projet

```
scraping-lead/
├── scripts/
│   ├── scraper_gmaps.py         # Google Maps
│   ├── scraper_pappers.py       # Pappers SCI/Sociétés
│   ├── lead_transfer.py         # Transfert GetSales/Brevo
│   ├── daily_recap.py           # Recap quotidien
│   └── scheduler.py             # Scheduler intelligent
├── services/
│   ├── brevo_service.py         # API Brevo
│   ├── getsales_service.py      # API GetSales
│   ├── gsheets_service.py       # Google Sheets
│   ├── supabase_client.py       # Supabase
│   └── resend_service.py        # Resend (emails)
├── services_metier/
│   └── scraper.py               # Scraper Selenium
├── config/
│   ├── gmaps_scraping_config.json
│   └── google_credentials.json
├── docker/
│   └── entrypoint.sh
├── docker-compose.yml
├── Dockerfile
└── requirements.txt