"""
seed_scrape_progress.py
-----------------------
Peuple la table scrape_progress avec toutes les villes à scraper
pour chaque niche.

Usage:
    python seed_scrape_progress.py                        # seed toutes les niches
    python seed_scrape_progress.py --niche serrurier      # seed une seule niche
    python seed_scrape_progress.py --reset                # supprime les pending et reseed tout
    python seed_scrape_progress.py --reset --niche hotel  # supprime les pending et reseed hotel

⚠️  --reset ne supprime JAMAIS les lignes 'done' ou 'error', uniquement 'pending'.
"""

import os
import sys
import argparse
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG SUPABASE
# ─────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    sys.exit("❌ SUPABASE_URL et SUPABASE_KEY sont requis")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────
# TOUS LES DÉPARTEMENTS FRANÇAIS
# ─────────────────────────────────────────────
ALL_DEPARTMENTS = [
    "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "2A", "2B",                                                   # Corse
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30",
    "31", "32", "33", "34", "35", "36", "37", "38", "39", "40",
    "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
    "51", "52", "53", "54", "55", "56", "57", "58", "59", "60",
    "61", "62", "63", "64", "65", "66", "67", "68", "69", "70",
    "71", "72", "73", "74", "75", "76", "77", "78", "79", "80",
    "81", "82", "83", "84", "85", "86", "87", "88", "89", "90",
    "91", "92", "93", "94", "95",
]

# ─────────────────────────────────────────────
# NICHES
# ─────────────────────────────────────────────
NICHES: dict[str, dict] = {
    "electricien":          {"label": "Électricien",                         "departments": ALL_DEPARTMENTS},
    "plombier":             {"label": "Plombier",                            "departments": ALL_DEPARTMENTS},
    "serrurier":            {"label": "Serrurier",                           "departments": ALL_DEPARTMENTS},
    "garage_auto":          {"label": "Garage automobile",                   "departments": ALL_DEPARTMENTS},
    "clinique_dentaire":    {"label": "Clinique dentaire",                   "departments": ALL_DEPARTMENTS},
    "architecte_interieur": {"label": "Architecte d'intérieur",              "departments": ALL_DEPARTMENTS},
    "architecte":           {"label": "Architecte",                          "departments": ALL_DEPARTMENTS},
    "hotel":                {"label": "Hôtel 5 étoiles",                     "departments": ALL_DEPARTMENTS},
    "cgp":                  {"label": "Conseiller en gestion de patrimoine", "departments": ALL_DEPARTMENTS},
    "comptable":            {"label": "Expert-comptable",                    "departments": ALL_DEPARTMENTS},
    "restaurant":           {"label": "Restaurant",                          "departments": ALL_DEPARTMENTS},
    "iad":                  {"label": "IAD",                                 "departments": ALL_DEPARTMENTS},
    "agent_immo":           {"label": "Agent immobilier",                    "departments": ALL_DEPARTMENTS},
}

BATCH_SIZE = 1000
PAGE_SIZE  = 1000


# ─────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────
def fetch_cities_for_departments(departments: list[str]) -> list[dict]:
    """Récupère toutes les villes avec pagination automatique."""
    all_cities = []
    chunk_size = 50  # évite les URLs trop longues
    for i in range(0, len(departments), chunk_size):
        chunk = departments[i : i + chunk_size]
        offset = 0
        while True:
            response = (
                supabase.table("cities")
                .select("id, name, department_code")
                .in_("department_code", chunk)
                .range(offset, offset + PAGE_SIZE - 1)
                .execute()
            )
            batch = response.data
            all_cities.extend(batch)
            if len(batch) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
    return all_cities


# ─────────────────────────────────────────────
# RESET
# ─────────────────────────────────────────────
def reset_pending(niche_key: str | None) -> int:
    """
    Supprime uniquement les lignes status='pending'.
    Les lignes 'done' et 'error' sont toujours préservées.
    """
    query = supabase.table("scrape_progress").delete().eq("status", "pending")
    if niche_key:
        query = query.eq("niche", niche_key)
    response = query.execute()
    return len(response.data)


# ─────────────────────────────────────────────
# SEED
# ─────────────────────────────────────────────
def insert_batch(rows: list[dict]) -> int:
    response = (
        supabase.table("scrape_progress")
        .upsert(rows, on_conflict="niche,city_id")
        .execute()
    )
    return len(response.data)


def seed_niche(niche_key: str, niche_config: dict) -> dict:
    label       = niche_config["label"]
    departments = niche_config["departments"]

    print(f"\n📍 [{niche_key}] {label} — {len(departments)} départements")

    cities = fetch_cities_for_departments(departments)
    print(f"   → {len(cities)} villes récupérées", end="", flush=True)

    if not cities:
        print(" ⚠️  Aucune ville trouvée, skip.")
        return {"niche": niche_key, "cities": 0, "inserted": 0}

    rows = [
        {"niche": niche_key, "city_id": city["id"], "status": "pending"}
        for city in cities
    ]

    total_inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        total_inserted += insert_batch(rows[i : i + BATCH_SIZE])
        print(".", end="", flush=True)

    print(f" ✅ {total_inserted} lignes insérées/mises à jour")
    return {"niche": niche_key, "cities": len(cities), "inserted": total_inserted}


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Seed scrape_progress")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Supprime les lignes 'pending' avant de réinsérer (done/error préservés)",
    )
    parser.add_argument(
        "--niche",
        type=str,
        default=None,
        help=f"Traiter une seule niche. Valeurs : {', '.join(NICHES.keys())}",
    )
    args = parser.parse_args()

    # Validation --niche
    if args.niche and args.niche not in NICHES:
        sys.exit(
            f"❌ Niche inconnue '{args.niche}'.\n"
            f"   Valeurs possibles : {', '.join(NICHES.keys())}"
        )

    niches_to_process = {args.niche: NICHES[args.niche]} if args.niche else NICHES

    print("=" * 55)
    print("  SEED scrape_progress")
    print("=" * 55)

    # ── Reset optionnel ──────────────────────
    if args.reset:
        scope = f"niche '{args.niche}'" if args.niche else "toutes les niches"
        print(f"\n⚠️  --reset activé ({scope})")
        print("   Seules les lignes 'pending' seront supprimées.")
        print("   Les lignes 'done' et 'error' sont préservées.\n")
        confirm = input("   Confirmer ? (oui/non) : ").strip().lower()
        if confirm != "oui":
            sys.exit("❌ Annulé.")
        deleted = reset_pending(args.niche)
        print(f"   🗑️  {deleted} lignes pending supprimées\n")

    # ── Seed ────────────────────────────────
    results = []
    for niche_key, niche_config in niches_to_process.items():
        results.append(seed_niche(niche_key, niche_config))

    # ── Résumé ──────────────────────────────
    print("\n" + "=" * 55)
    print("  RÉSUMÉ")
    print("=" * 55)

    for r in results:
        icon = "✅" if r["inserted"] > 0 else "⚠️ "
        print(f"  {icon} {r['niche']:<25} {r['cities']:>6} villes  →  {r['inserted']:>6} lignes")

    print("-" * 55)
    print(
        f"  TOTAL {len(results)} niches"
        f" — {sum(r['cities'] for r in results)} villes"
        f" — {sum(r['inserted'] for r in results)} lignes"
    )
    print("=" * 55)


if __name__ == "__main__":
    main()
