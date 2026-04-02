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
    
    def create_lead(self, payload: dict):
        """
        Insert a lead into the Supabase database using direct HTTP requests.
        Handles conflicts by updating existing records based on unique constraints.
        Supports automatic retry on 429 Rate Limit.
        """
        import time
        max_retries = 3
        retry_delay = 2

        lead_data = {
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
        }
        
        lead_data = {k: v for k, v in lead_data.items() if v is not None}
        url = f"{self.base_url}/leads"
        
        # resolution=merge-duplicates handles 409 by updating existing record
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
                    raise Exception(f"Failed to insert lead (HTTP {e.response.status_code}): {e.response.text}")
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Failed to insert lead: {str(e)}")
                time.sleep(retry_delay)

    def update_lead(self, query_params: dict, update_data: dict):
        """
        Update a lead based on query parameters (e.g., phone or company).
        """
        import time
        max_retries = 3
        retry_delay = 2

        query_str = "&".join([f"{k}=eq.{v}" for k, v in query_params.items()])
        url = f"{self.base_url}/leads?{query_str}"
        
        for attempt in range(max_retries):
            try:
                response = requests.patch(url, json=update_data, headers=self.headers, timeout=20)
                
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                
                response.raise_for_status()
                return True
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                time.sleep(retry_delay)
        return False
