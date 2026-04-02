#!/usr/bin/env python3
"""
Service pour interagir directement avec l'API GetSales.
"""

import os
import json
import urllib.request
import urllib.error
from typing import Any, Dict, Optional, List

class GetSalesService:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://amazing.getsales.io",  # URL correcte pour amazing.getsales.io
        list_uuid: Optional[str] = None,
        debug: bool = True
    ):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.list_uuid = list_uuid
        self.debug = debug

    def _make_request(self, method: str, endpoint: str, body: Dict[str, Any], attempt_fallback: bool = True) -> Dict[str, Any]:
        url = f"{self.base_url}{endpoint}"
        data = json.dumps(body).encode("utf-8")
        
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Content-Type", "application/json; charset=utf-8")
        req.add_header("Accept", "application/json")
        req.add_header("Authorization", f"Bearer {self.api_key}")
        # Ajouter un User-Agent pour éviter le blocage Cloudflare (403)
        req.add_header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                response_body = resp.read().decode("utf-8", errors="replace")
                return json.loads(response_body) if response_body else {}
        except urllib.error.HTTPError as e:
            # Gérer la redirection ou le mauvais endpoint (404)
            if e.code == 404 and attempt_fallback and "/leads/api" in endpoint:
                fallback_endpoint = endpoint.replace("/leads/api", "/api")
                return self._make_request(method, fallback_endpoint, body, attempt_fallback=False)
            
            err_body = e.read().decode("utf-8", errors="replace")
            
            # Tenter de parser l'erreur JSON
            try:
                err_json = json.loads(err_body)
                raise Exception(f"GetSales API Error ({e.code}) at {url}: {err_json.get('message', err_body[:500])}")
            except:
                raise Exception(f"GetSales API Error ({e.code}) at {url}: {err_body[:500]}")
        except Exception as e:
            raise e

    def lookup_lead(self, linkedin_id: str) -> Optional[Dict[str, Any]]:
        """Vérifie si un lead existe déjà via son LinkedIn ID"""
        body = {
            "linkedin_id": linkedin_id,
            "disable_aggregation": True
        }
        
        try:
            # L'endpoint de lookup renvoie souvent le lead directement ou null/404
            result = self._make_request("POST", "/leads/api/leads/lookup-one", body)
            return result if result else None
        except Exception as e:
            # Si c'est une 404, on considère que le lead n'existe pas
            if "404" in str(e):
                return None
            raise e

    def upsert_lead(self, lead: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Crée ou met à jour un lead"""
        payload = {
            "lead": lead
        }
        
        if options:
            payload.update(options)
            
        # Injecter le list_uuid par défaut si présent et non fourni dans options
        if self.list_uuid and (not options or "list_uuid" not in options):
            payload["list_uuid"] = self.list_uuid

        return self._make_request("POST", "/leads/api/leads/upsert", payload)

    def add_lead_to_flow(self, lead_uuid: str, flow_uuid: Optional[str] = None) -> Dict[str, Any]:
        """Ajoute un lead à une automatisation (flow)"""
        f_uuid = flow_uuid or os.environ.get("GETSALES_FLOW_ADD_SCI_OWNER_UUID")
        if not f_uuid:
            return {}

        endpoint = f"/flows/api/flows/{f_uuid}/leads/{lead_uuid}"
        return self._make_request("POST", endpoint, {}, attempt_fallback=False)

    def search_leads(self, list_uuid: str, created_at_from: str, created_at_to: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Recherche des leads dans une liste spécifique pour une plage de dates.
        
        Args:
            list_uuid: UUID de la liste GetSales
            created_at_from: Date de début au format YYYY-MM-DD
            created_at_to: Date de fin au format YYYY-MM-DD
            limit: Nombre maximum de résultats à retourner
            
        Returns:
            Liste de leads correspondant aux critères.
        """
        all_leads = []
        offset = 0
        per_page = min(limit, 100) if limit else 100

        while True:
            payload = {
                "filter": {
                    "list_uuid": list_uuid,
                    "created_at": {
                        "from": created_at_from,
                        "to": created_at_to
                    }
                },
                "limit": per_page,
                "offset": offset,
                "order_field": "created_at",
                "order_type": "desc",
                "disable_aggregation": True
            }

            try:
                import logging
                logger = logging.getLogger(__name__)
                
                # The endpoint is /leads/api/leads/search with POST method
                result = self._make_request("POST", "/leads/api/leads/search", payload)
                
                leads = result.get("leads") or result.get("data") or result.get("items") or []
                if isinstance(result, list):
                    leads = result

                if not leads:
                    break

                all_leads.extend(leads)
                if self.debug:
                    print(f"  GetSales search offset {offset}: {len(leads)} leads")

                if len(leads) < per_page:
                    break
                
                # Stop if we reached the limit
                if limit and len(all_leads) >= limit:
                    all_leads = all_leads[:limit]
                    break
                    
                offset += len(leads)
                import time
                time.sleep(0.5)

            except Exception as e:
                if self.debug:
                    print(f"  GetSales search error: {e}")
                break

        return all_leads

    def queue_enrichment(self, list_uuid: Optional[str] = None, uuids: Optional[List[str]] = None) -> Dict[str, Any]:
        """Ajouter des contacts a la file d'enrichissement LinkedIn.

        Utilise PUT /leads/api/leads/advanced-enrichment.
        Passe soit un filtre par liste, soit des UUIDs specifiques.

        Args:
            list_uuid: UUID de la liste a enrichir (enrichit tous les contacts need_enrichment)
            uuids: Liste d'UUIDs specifiques a enrichir
        """
        body: Dict[str, Any] = {}

        if uuids:
            body["uuids"] = uuids
        elif list_uuid:
            body["filter"] = {
                "all": True,
                "ids": [],
                "leadFilter": {
                    "list_uuid": list_uuid,
                    "linkedin_status": "need_enrichment"
                }
            }
        else:
            target_list = self.list_uuid
            if not target_list:
                raise ValueError("list_uuid ou uuids requis pour l'enrichissement")
            body["filter"] = {
                "all": True,
                "ids": [],
                "leadFilter": {
                    "list_uuid": target_list,
                    "linkedin_status": "need_enrichment"
                }
            }

        return self._make_request("PUT", "/leads/api/leads/advanced-enrichment", body)

    def process_lead(self, lead: Dict[str, Any], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Logique complète : check doublon puis upsert si nouveau.
        Reproduit la logique du Controller TypeScript.
        """
        linkedin_id = lead.get("linkedin_id") or lead.get("linkedin") or lead.get("ln_id")
        
        if not linkedin_id:
            raise ValueError("linkedin_id (ou linkedin/ln_id) est requis pour l'envoi vers GetSales")

        # 1. Vérifier si le contact existe déjà
        existing = self.lookup_lead(linkedin_id)
        if existing:
            return {"status": "exists", "data": existing}

        # 2. Créer le lead
        created = self.upsert_lead(lead, options)
        
        # 3. Récupérer l'UUID du lead créé
        lead_data = created.get("lead", {})
        lead_uuid = lead_data.get("uuid")
        
        if lead_uuid:
            # 4. Ajouter à la file d'enrichissement
            try:
                self.queue_enrichment(uuids=[lead_uuid])
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Erreur lors de l'ajout à l'enrichissement pour {lead_uuid}: {e}")
                
            # 5. Ajouter à l'automatisation (flow) pour le Add SCI Owner
            try:
                flow_uuid = os.environ.get("GETSALES_FLOW_ADD_SCI_OWNER_UUID")
                if flow_uuid:
                    self.add_lead_to_flow(lead_uuid, flow_uuid=flow_uuid)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Erreur lors de l'ajout au flow pour {lead_uuid}: {e}")

        return {"status": "created", "data": created}
