import os
import re
import httpx

WASENDER_API_BASE = "https://www.wasenderapi.com/api"


class WasenderService:
    def __init__(self):
        self.api_key = os.getenv("WASENDER_API_KEY")
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _normalize(self, numero: str) -> str:
        digits = re.sub(r"\D", "", numero)
        return f"+{digits}"

    def _is_session_error(self, exc: Exception) -> bool:
        if isinstance(exc, httpx.HTTPStatusError):
            return exc.response.status_code in (401, 403)
        return False

    def check_whatsapp(self, numero: str) -> bool:
        normalized = self._normalize(numero)
        url = f"{WASENDER_API_BASE}/on-whatsapp/{normalized}"
        try:
            with httpx.Client(headers=self.headers, timeout=15) as client:
                resp = client.get(url)
                resp.raise_for_status()
                data = resp.json()
                exists = bool(data.get("data", {}).get("exists", False))
                print(f"[Wasender] {normalized} → {'OUI' if exists else 'NON'}")
                return exists
        except Exception as exc:
            if self._is_session_error(exc):
                print(f"[Wasender] ⚠️ Session expirée — reconnectez-vous sur wasenderapi.com")
            print(f"[Wasender] Erreur check {normalized} : {exc}")
            return False
