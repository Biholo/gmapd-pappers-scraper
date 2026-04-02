# ============================================
# Makefile - Scraping Lead
# Commandes rapides pour gérer les conteneurs
# ============================================

.PHONY: build run-gmaps run-sci scheduler stop logs clean help

# Image Docker
IMAGE_NAME=scraping-lead
IMAGE_TAG=latest

## Build l'image Docker
build:
	docker compose build

## Lancer le scraper Google Maps (one-shot)
run-gmaps:
	docker compose run --rm gmaps

## Lancer le scraper SCI/Pappers (one-shot)
run-sci:
	docker compose run --rm sci

## Démarrer le scheduler (cron, tourne en continu)
scheduler:
	docker compose up -d scheduler

## Arrêter tous les conteneurs
stop:
	docker compose down

## Voir les logs en temps réel
logs:
	docker compose logs -f

## Logs d'un service spécifique (usage: make logs-gmaps)
logs-%:
	docker compose logs -f $*

## Ouvrir un shell dans le conteneur
shell:
	docker compose run --rm --entrypoint bash gmaps

## Nettoyer les images et conteneurs
clean:
	docker compose down --rmi local -v
	docker image prune -f

## Afficher l'aide
help:
	@echo "=== Scraping Lead - Commandes ==="
	@echo ""
	@echo "  make build       Build l'image Docker"
	@echo "  make run-gmaps   Lancer scraper Google Maps"
	@echo "  make run-sci     Lancer scraper SCI/Pappers"
	@echo "  make scheduler   Démarrer le cron scheduler"
	@echo "  make stop        Arrêter tous les conteneurs"
	@echo "  make logs        Voir les logs en temps réel"
	@echo "  make shell       Shell interactif dans le conteneur"
	@echo "  make clean       Nettoyer images et conteneurs"
	@echo ""
