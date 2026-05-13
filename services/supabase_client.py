import os
import requests
from typing import Optional

class SupabaseClient:
    def __init__(self, url: str = None, key: str = None):
        self.url = url or os.getenv("SUPABASE_URL")
        self.key = key or os.getenv("SUPABASE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be provided")
        
        self.base_url = f"{self.url}/rest/v1"
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
    
    def get_cities_by_department(self, department_code: str, min_population: int = 5000):
        """
        Fetch all cities for a given department code from Supabase.
        Filters cities by minimum population (default: 5000 inhabitants).
        Returns a list of city dictionaries with id, name, postal_code, etc.
        """
        try:
            url = f"{self.base_url}/cities?department_code=eq.{department_code}&pop=gte.{min_population}&select=id,name,postal_code,department_code,pop"
            response = requests.get(url, headers=self.headers, timeout=20)
            response.raise_for_status()
            
            cities = response.json()
            return cities
        except requests.HTTPError as e:
            raise Exception(f"Failed to fetch cities (HTTP {e.response.status_code}): {e.response.text if e.response else str(e)}")
        except Exception as e:
            raise Exception(f"Failed to fetch cities: {str(e)}")
    
    def _build_lead_data(self, payload: dict) -> dict:
        return {
            "company": payload.get("company"),
            "niche": payload.get("niche"),
            "city_id": payload.get("cityId"),
            "address": payload.get("address"),
            "phone": payload.get("phone"),
            "email": payload.get("email"),
            "website_url": payload.get("webSiteUrl"),
            "facebook_url": payload.get("facebookUrl"),
            "x_url": payload.get("xUrl"),
            "linkedin_url": payload.get("linkedinUrl"),
            "instagram_url": payload.get("instagramUrl"),
            "number_of_rate": payload.get("numberOfRate"),
            "average_rate": payload.get("averageRate"),
            "is_mobile_phone": payload.get("is_mobile_phone"),
            "google_category": payload.get("google_category"),
            "google_maps_url": payload.get("google_maps_url"),
        }

    def _find_lead_by_phone(self, phone: str):
        try:
            resp = requests.get(
                f"{self.base_url}/leads",
                params={"phone": f"eq.{phone}", "select": "id,phone", "limit": "1"},
                headers=self.headers,
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            return data[0] if data else None
        except Exception:
            return None

    def create_lead(self, payload: dict):
        """
        Upsert a lead: update by phone if exists, otherwise insert.
        Supports automatic retry on 429 Rate Limit.
        """
        import time
        max_retries = 3
        retry_delay = 2

        lead_data = {k: v for k, v in self._build_lead_data(payload).items() if v is not None}
        phone = lead_data.get("phone")

        # Update existing lead if phone matches
        if phone:
            existing = self._find_lead_by_phone(phone)
            if existing:
                try:
                    self.update_lead({"phone": phone}, lead_data)
                    return {"status": "updated"}
                except Exception as e:
                    raise Exception(f"Failed to update lead by phone: {str(e)}") from e

        url = f"{self.base_url}/leads"
        upsert_headers = {
            **self.headers,
            "Prefer": "return=representation,resolution=merge-duplicates"
        }

        for attempt in range(max_retries):
            try:
                response = requests.post(url, json=lead_data, headers=upsert_headers, timeout=20)

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue

                response.raise_for_status()
                return response.json() if response.content else {"status": "created"}

            except requests.HTTPError as e:
                if e.response.status_code == 409:
                    return {"status": "duplicate", "message": "Handled by merge-duplicates"}
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to insert lead (HTTP {e.response.status_code}): {e.response.text}") from e
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to insert lead: {str(e)}") from e
                time.sleep(retry_delay)

    def update_lead(self, query_params: dict, update_data: dict):
        """
        Update a lead based on query parameters (e.g., phone or company).
        """
        import time
        max_retries = 3
        retry_delay = 2

        url = f"{self.base_url}/leads"
        filter_params = {k: f"eq.{v}" for k, v in query_params.items()}

        for attempt in range(max_retries):
            try:
                response = requests.patch(url, params=filter_params, json=update_data, headers=self.headers, timeout=20)

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue

                response.raise_for_status()
                return True
            except requests.HTTPError as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Failed to update lead (HTTP {e.response.status_code}): {e.response.text[:500]}"
                    ) from e
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                time.sleep(retry_delay)
        return False

    def get_pending_missions(self, limit=500):
        """Fetch next N pending scrape missions ordered deterministically."""
        url = (
            f"{self.base_url}/scrape_progress"
            f"?select=niche,city_id,cities(name,department_code)"
            f"&status=eq.pending"
            f"&order=niche.asc,city_id.asc"
            f"&limit={limit}"
        )
        try:
            response = requests.get(url, headers=self.headers, timeout=20)
            response.raise_for_status()
            rows = response.json()
            missions = []
            for row in rows:
                city = row.get("cities") or {}
                missions.append({
                    "niche": row["niche"],
                    "city_id": row["city_id"],
                    "name": city.get("name"),
                    "department_code": city.get("department_code") or "",
                })
            return missions
        except requests.HTTPError as e:
            raise Exception(f"Failed to fetch pending missions (HTTP {e.response.status_code}): {e.response.text}") from e
        except Exception as e:
            raise Exception(f"Failed to fetch pending missions: {str(e)}") from e

    def claim_mission(self, niche: str, city_id: int) -> bool:
        """Atomically claim a pending mission by setting status to in_progress.
        Returns True if claimed, False if another worker claimed it first."""
        from datetime import datetime, timezone
        url = f"{self.base_url}/scrape_progress?niche=eq.{niche}&city_id=eq.{city_id}&status=eq.pending"
        body = {
            "status": "in_progress",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            response = requests.patch(url, json=body, headers=self.headers, timeout=10)
            response.raise_for_status()
            return len(response.json()) > 0
        except Exception:
            return False

    def update_scrape_progress(self, niche: str, city_id: int, status: str,
                                leads_found: int = None, error_msg: str = None):
        """Update scrape_progress row after scraping a city."""
        from datetime import datetime, timezone
        url = f"{self.base_url}/scrape_progress?niche=eq.{niche}&city_id=eq.{city_id}"
        body = {
            "status": status,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
        }
        if leads_found is not None:
            body["leads_found"] = leads_found
        if error_msg:
            body["error_msg"] = error_msg[:500]
        try:
            response = requests.patch(url, json=body, headers=self.headers, timeout=20)
            response.raise_for_status()
        except Exception as e:
            raise Exception(f"Failed to update scrape_progress: {str(e)}") from e
