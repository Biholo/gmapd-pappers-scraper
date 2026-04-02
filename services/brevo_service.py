import os
import time
import requests
from typing import Any, Dict, List, Optional


class BrevoService:
    BASE_URL = "https://api.brevo.com/v3"

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("BREVO_API_KEY")
        if not self.api_key:
            raise ValueError("BREVO_API_KEY must be provided")
        self.headers = {
            "api-key": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _request(self, method: str, endpoint: str, params: dict = None, json_body: dict = None, max_retries: int = 3) -> Optional[dict]:
        url = f"{self.BASE_URL}{endpoint}"
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                response = requests.request(
                    method, url,
                    headers=self.headers,
                    params=params,
                    json=json_body,
                    timeout=20,
                )

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue

                response.raise_for_status()
                return response.json() if response.content else None

            except requests.HTTPError as e:
                if attempt == max_retries - 1:
                    raise Exception(
                        f"Brevo API Error ({e.response.status_code}): {e.response.text[:500]}"
                    )
            except Exception as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Brevo API Error: {str(e)}")
                time.sleep(retry_delay)

    # ── Contacts ──────────────────────────────────────────

    def create_contact(
        self,
        email: str = None,
        attributes: Dict[str, Any] = None,
        list_ids: List[int] = None,
        ext_id: str = None,
        update_enabled: bool = False,
    ) -> Optional[dict]:
        body = {}
        if email:
            body["email"] = email
        if attributes:
            body["attributes"] = attributes
        if list_ids:
            body["listIds"] = list_ids
        if ext_id:
            body["ext_id"] = ext_id
        if update_enabled:
            body["updateEnabled"] = update_enabled

        return self._request("POST", "/contacts", json_body=body)

    def update_contact(
        self,
        identifier: str,
        attributes: Dict[str, Any] = None,
        list_ids: List[int] = None,
        unlink_list_ids: List[int] = None,
        email_blacklisted: bool = None,
        sms_blacklisted: bool = None,
        ext_id: str = None,
        identifier_type: str = None,
    ) -> None:
        body = {}
        if attributes:
            body["attributes"] = attributes
        if list_ids:
            body["listIds"] = list_ids
        if unlink_list_ids:
            body["unlinkListIds"] = unlink_list_ids
        if email_blacklisted is not None:
            body["emailBlacklisted"] = email_blacklisted
        if sms_blacklisted is not None:
            body["smsBlacklisted"] = sms_blacklisted
        if ext_id:
            body["ext_id"] = ext_id

        params = {}
        if identifier_type:
            params["identifierType"] = identifier_type

        self._request("PUT", f"/contacts/{identifier}", params=params or None, json_body=body)

    def get_contacts(
        self,
        limit: int = 50,
        offset: int = 0,
        list_ids: List[int] = None,
        sort: str = "desc",
        modified_since: str = None,
        created_since: str = None,
    ) -> Optional[dict]:
        params = {"limit": limit, "offset": offset, "sort": sort}
        if list_ids:
            params["listIds"] = ",".join(str(i) for i in list_ids)
        if modified_since:
            params["modifiedSince"] = modified_since
        if created_since:
            params["createdSince"] = created_since

        return self._request("GET", "/contacts", params=params)
