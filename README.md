# Scraping Lead - Pipeline de prospection automatisee

Systeme complet de scraping, enrichissement LinkedIn et envoi vers Brevo.
3 pipelines : Google Maps, Pappers (SCI + Societes), Transfert GetSales/Brevo.

## Architecture

```
Google Maps  ──────> Supabase + Brevo (direct si email)
Pappers SCI  ──────> GetSales (enrichissement LinkedIn) ──────> Brevo
Pappers SASU/SAS ──> GetSales (enrichissement LinkedIn) ──────> Brevo
```

### Deux comptes Brevo

| Compte | API Key env | Niches |
|--------|-------------|--------|
| **Opti Habitat** | `BREVO_OPTI_HABITAT_API_KEY` | Agent immo, CGP, Marchand de biens, SCI |
| **Develly** | `BREVO_DEVELLY_API_KEY` | Comptables, Restaurants asiatiques, Serruriers, Plombiers, Electriciens |

---

## Installation

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

## Scripts

### 1. Google Maps - `scripts/scraper_gmaps.py`

Scrape toute la France par niche sur Google Maps. Envoie directement vers Brevo quand un email est trouve.

```bash
# Lancer le scraping (utilise config/gmaps_scraping_config.json)
python scripts/scraper_gmaps.py
```

**Configuration** : `config/gmaps_scraping_config.json`

Chaque niche contient :
- `name` : Nom de la niche
- `search_query` : Requete Google Maps
- `brevo_api_key_env` : Variable env de la cle API Brevo
- `brevo_list_id_env` : Liste de variables env des IDs de listes Brevo (array)
- `departments` : Departements restants a scraper (retires automatiquement quand termines)

**Parametres** dans `config/config.py` :
- `MAX_SCROLLS` : Nombre max de scrolls par ville (defaut: 50)
- `HEADLESS` : Mode headless (defaut: True)
- `DELAY_BETWEEN_CITIES` : Delai entre villes en secondes (defaut: 2)
- `DELAY_BETWEEN_NICHES` : Delai entre niches (defaut: 5)

**Anti-doublons** : Validation email par regex stricte + dedup local en memoire + `update_enabled=True` sur Brevo (met a jour si le contact existe deja).

---

### 2. Pappers (SCI / Societes) - `scripts/scraper_pappers.py`

Scrape les entreprises sur Pappers.fr, enrichit les dirigeants via Yahoo/LinkedIn, envoie vers GetSales puis file d'enrichissement LinkedIn.

```bash
# Scraping SCI (forme juridique 6540)
python scripts/scraper_pappers.py --type sci --mode production --send-email

# Scraping Societes SASU/SAS/EURL/SARL (formes 5720,5710,5498,5499)
python scripts/scraper_pappers.py --type company --mode production --send-email

# Mode test (2 pages max, headless)
python scripts/scraper_pappers.py --type sci --mode test

# Mode visible (navigateur affiche)
python scripts/scraper_pappers.py --type company --mode visible

# Mode illimite (pas de limite de pages)
python scripts/scraper_pappers.py --type sci --mode unlimited

# Limiter le nombre de pages
python scripts/scraper_pappers.py --type sci --mode production --max-pages 10

# Menu interactif (sans --mode)
python scripts/scraper_pappers.py --type sci
```

**Parametres** :

| Parametre | Valeurs | Description |
|-----------|---------|-------------|
| `--type` | `sci`, `company` | Type de scraping (defaut: `sci`) |
| `--mode` | `test`, `visible`, `unlimited`, `production`, `custom` | Mode d'execution |
| `--max-pages` | entier | Limite de pages par date |
| `--send-email` | flag | Envoyer un rapport par email a la fin |
| `--email-to` | adresses | Destinataires du rapport |

**Routing GetSales** :

| Type | Condition | Liste GetSales |
|------|-----------|---------------|
| SCI | Creee < 30 jours | `GETSALES_NEW_SCI_LIST_UUID` |
| SCI | Creee > 30 jours | `GETSALES_OLD_SCI_LIST_UUID` |
| Company | Toujours | `GETSALES_NEW_COMPANY_LIST_UUID` |

Apres ajout dans GetSales, le lead est automatiquement ajoute a la **file d'enrichissement LinkedIn** (API `PUT /leads/api/leads/advanced-enrichment`).

**Config persistante** : `scraper_config.json` (SCI) / `scraper_config_company.json` (societes) - sauvegarde la page et la date pour reprendre en cas d'interruption.

---

### 3. Transfert de leads - `scripts/lead_transfer.py`

Transfère les leads enrichis entre GetSales et Brevo.

#### GetSales -> Brevo (leads avec email uniquement)

```bash
python scripts/lead_transfer.py getsales-to-brevo
```

**Mapping** :

| Liste GetSales | Listes Brevo |
|---------------|-------------|
| `GETSALES_NEW_SCI_LIST_UUID` | `BREVO_SCI_LOGICIEL_LIST` + `BREVO_SCI_NEW_LIST` |
| `GETSALES_OLD_SCI_LIST_UUID` | `BREVO_SCI_LOGICIEL_LIST` + `BREVO_SCI_OLD_LIST` |

#### Brevo liste -> liste (avec limite quotidienne)

Transfere des contacts d'une liste de stockage vers une liste rattachee a un workflow, avec un max/jour pour eviter le flag du domaine.

```bash
# Transferer max 100 contacts/jour de la liste 5 vers la liste 12 (Opti Habitat)
python scripts/lead_transfer.py brevo-transfer --from-list 5 --to-list 12 --max-per-day 100

# Avec la cle Develly
python scripts/lead_transfer.py brevo-transfer --from-list 5 --to-list 12 --max-per-day 100 --api-key-env BREVO_DEVELLY_API_KEY
```

**Parametres** :

| Parametre | Description |
|-----------|-------------|
| `--from-list` | ID de la liste source |
| `--to-list` | ID de la liste destination |
| `--max-per-day` | Max contacts transferes par jour (defaut: 100) |
| `--api-key-env` | Variable env de la cle API (defaut: `BREVO_OPTI_HABITAT_API_KEY`) |

---

### 4. Recap quotidien - `scripts/daily_recap.py`

Affiche ou envoie par email un recapitulatif de la journee.

```bash
# Afficher le recap dans la console
python scripts/daily_recap.py

# Envoyer par email
python scripts/daily_recap.py --send-email

# Avec destinataires specifiques
python scripts/daily_recap.py --send-email --email-to user@example.com
```

**Stats trackees** : leads scrapes (GMaps + Pappers), envoyes Brevo, envoyes GetSales, enrichissements files, transferts liste-a-liste.

Stockees dans `state/daily_stats.json`, remises a zero chaque jour.

---

## Docker

### Build

```bash
docker compose build
# ou
make build
```

### Lancer un scraper

```bash
# Google Maps
docker compose run --rm gmaps

# SCI Pappers
docker compose run --rm sci

# Societes (SASU/SAS/EURL/SARL)
docker compose run --rm company

# Transfert GetSales -> Brevo
docker compose run --rm transfer

# Transfert Brevo liste -> liste
docker compose run --rm scraper-scheduler brevo-transfer --from-list 5 --to-list 12

# Recap quotidien
docker compose run --rm scraper-scheduler recap --send-email
```

### Scheduler (cron automatique)

```bash
docker compose up -d scheduler
```

**Variables d'environnement cron** :

| Variable | Defaut | Description |
|----------|--------|-------------|
| `CRON_GMAPS` | `0 6 * * *` | Google Maps a 6h |
| `CRON_SCI` | `0 8 * * *` | SCI Pappers a 8h |
| `CRON_COMPANY` | `0 10 * * *` | Societes a 10h |
| `CRON_TRANSFER` | `0 14 * * *` | Transfert GetSales->Brevo a 14h |
| `CRON_RECAP` | `0 22 * * *` | Recap quotidien a 22h |

### Commandes Docker disponibles

| Commande | Description |
|----------|-------------|
| `gmaps` | Scraper Google Maps |
| `sci` | Scraper SCI/Pappers |
| `company` | Scraper Societes (SASU/SAS/EURL/SARL) |
| `transfer` | Transfert GetSales -> Brevo |
| `brevo-transfer` | Transfert Brevo liste -> liste |
| `recap` | Recap quotidien |
| `cron` | Scheduler cron |
| `shell` | Shell interactif |

---

## Variables d'environnement

```env
# GetSales
GETSALES_BASE_URL=https://amazing.getsales.io
GETSALES_API_KEY=...
GETSALES_FLOW_UUID=...
GETSALES_NEW_SCI_LIST_UUID=cae70689-...
GETSALES_OLD_SCI_LIST_UUID=def11074-...
GETSALES_NEW_COMPANY_LIST_UUID=82663f51-...

# Brevo (2 comptes)
BREVO_OPTI_HABITAT_API_KEY=xkeysib-...
BREVO_DEVELLY_API_KEY=xkeysib-...

# Opti Habitat listes
BREVO_SCI_LOGICIEL_LIST=9
BREVO_SCI_OLD_LIST=6
BREVO_SCI_NEW_LIST=5
BREVO_AGENT_IMMO_LIST=4
BREVO_CGP_LIST=8
BREVO_MARCHAND_BIENS_LIST=10

# Develly listes
BREVO_COMPTABLE_RUNING_LIST=12
BREVO_ASTIATIQUE_RUNING_LIST=11
BREVO_COMPTABLE_LIST=5
BREVO_ASTIATIQUE_LIST=6
BREVO_SERRURIER_LIST=16
BREVO_PLUMBER_LIST=15
BREVO_ELECTRICIAN_LIST=14

# Resend (rapports email)
RESEND_API_KEY=...
RESEND_FROM_EMAIL=...
RESEND_TO_EMAIL=...

# Supabase
SUPABASE_URL=...
SUPABASE_KEY=...
```

---

## Structure du projet

```
scraping-lead/
├── scripts/
│   ├── scrape_departments.py    # Google Maps scraper
│   ├── scraper_enhanced.py      # Pappers SCI/Societes scraper
│   ├── lead_transfer.py         # Transfert GetSales/Brevo
│   └── daily_recap.py           # Recap quotidien
├── services/
│   ├── brevo_service.py         # API Brevo
│   ├── getsales_service.py      # API GetSales
│   ├── supabase_client.py       # API Supabase
│   └── resend_service.py        # API Resend (emails)
├── services-metier/
│   └── scraper.py               # Scraper Google Maps Selenium
├── config/
│   ├── config.py                # Parametres scraping
│   └── gmaps_scraping_config.json  # Config niches/departements
├── state/
│   ├── daily_stats.json         # Stats quotidiennes
│   ├── transfer_state.json      # Compteurs transferts
│   └── .scraped_history.json    # Historique scraping GMaps
├── docker/
│   └── entrypoint.sh            # Entrypoint Docker
├── docker-compose.yml
├── Dockerfile
├── Makefile
├── requirements.txt
└── .env
```
#   g m a p d - p a p p e r s - s c r a p e r  
 