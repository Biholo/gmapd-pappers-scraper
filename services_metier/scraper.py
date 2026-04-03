"""
Scraper Google Maps simple avec Selenium (sans Scrapy)
"""
import os
import json
import re
import time
import logging
import requests
from urllib.parse import urlparse, urljoin
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys

from services.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

CONSENT_BUTTON_XPATHS = [
    "//button[contains(., 'Tout accepter') or contains(., 'Accepter') or contains(., 'Accept all') or contains(., 'I agree')]",
    "//button//*[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'accepter')]/ancestor::button",
    "//div[@role='dialog']//button",
]


class GoogleMapsScraper:
    def __init__(self, headless=False, max_scrolls=50):
        self.headless = headless
        self.max_scrolls = max_scrolls
        self.driver = None
        self.supabase = None
        self.cities_scraped = 0  # Compteur pour recycler le driver
        self.max_cities_per_driver = 10  # Recycler tous les 10 villes
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        logs_dir = os.path.join(base_dir, 'logs')
        os.makedirs(logs_dir, exist_ok=True)
        self.history_path = os.path.join(logs_dir, ".scraped_history.json")

        try:
            self.supabase = SupabaseClient()
            logger.info("Supabase client initialized")
        except Exception as e:
            logger.warning(f"Supabase client init failed: {e}")

    def _load_history(self):
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def is_already_scraped(self, city_id, niche):
        data = self._load_history()
        city_key = str(city_id)
        return niche in data.get(city_key, [])

    def _build_driver(self):
        opts = ChromeOptions()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--start-maximized")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--lang=fr-FR")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        # Optimisations mémoire et stabilité
        opts.add_argument("--disable-extensions")
        opts.add_argument("--disable-plugins")
        opts.add_argument("--disable-images")  # Désactiver les images pour économiser la mémoire
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-sync")
        opts.add_argument("--disable-translate")
        opts.add_argument("--disable-background-networking")
        opts.add_argument("--disable-breakpad")
        opts.add_argument("--disable-client-side-phishing-detection")
        opts.add_argument("--disable-component-extensions-with-background-pages")
        opts.add_argument("--disable-default-apps")
        opts.add_argument("--disable-device-discovery-notifications")
        opts.add_argument("--disable-hang-monitor")
        opts.add_argument("--disable-popup-blocking")
        opts.add_argument("--disable-prompt-on-repost")
        opts.add_argument("--disable-background-timer-throttling")
        opts.add_argument("--disable-renderer-backgrounding")
        opts.add_argument("--disable-preconnect")
        # Limite mémoire
        opts.add_argument("--memory-pressure-off")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)

        # Support Docker : utiliser Chromium si CHROME_BIN est défini
        chrome_bin = os.environ.get("CHROME_BIN")
        if chrome_bin:
            opts.binary_location = chrome_bin

        chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")
        service = ChromeService(executable_path=chromedriver_path) if chromedriver_path else ChromeService()
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(30)  # Timeout de 30s pour charger une page
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        })
        return driver

    def _accept_consent(self, driver, timeout=8):
        try:
            for xp in CONSENT_BUTTON_XPATHS:
                try:
                    btn = WebDriverWait(driver, timeout).until(
                        EC.element_to_be_clickable((By.XPATH, xp))
                    )
                    btn.click()
                    time.sleep(0.8)
                    logger.debug("Consent banner accepted")
                    return True
                except Exception:
                    continue
        except Exception:
            pass
        return False

    def _scroll_feed(self, driver):
        try:
            feed = driver.find_elements(By.CSS_SELECTOR, "div[role='feed']")
            if not feed:
                raise Exception("Feed not found")
            feed = feed[0]
        except Exception:
            self._accept_consent(driver)
            feed = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
            )

        previous_count = 0
        no_change_count = 0
        max_attempts = max(self.max_scrolls, 50)

        for i in range(max_attempts):
            driver.execute_script("arguments[0].scrollBy(0, arguments[0].clientHeight);", feed)
            time.sleep(0.5)

            current_count = len(driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK"))

            if current_count == previous_count:
                no_change_count += 1
                if no_change_count >= 5:
                    logger.info(f"  Fin de liste apres {i+1} scrolls ({current_count} cards)")
                    break
            else:
                no_change_count = 0

            previous_count = current_count

    def _extract_card_details(self, driver, expected_name=None):
        data = {
            "name": None, "address": None, "phone": None,
            "website": None, "averageRate": None, "numberOfRate": None,
        }

        try:
            try:
                name_elem = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "h1.DUwDvf"))
                )
                if name_elem and name_elem.text.strip():
                    data["name"] = name_elem.text.strip()
            except Exception:
                pass

            buttons = driver.find_elements(By.CSS_SELECTOR, "button[data-item-id], a[data-item-id]")
            for btn in buttons:
                item_id = btn.get_attribute("data-item-id") or ""
                
                if item_id == "address" and not data["address"]:
                    try:
                        data["address"] = btn.find_element(By.CSS_SELECTOR, "div.Io6YTe").text.strip()
                    except: pass
                
                elif item_id.startswith("phone:") and not data["phone"]:
                    phone_match = re.search(r"phone:tel:(\d+)", item_id)
                    if phone_match:
                        raw = phone_match.group(1)
                        if len(raw) == 10:
                            data["phone"] = " ".join([raw[i:i+2] for i in range(0, 10, 2)])
                        else:
                            data["phone"] = raw
                    else:
                        try:
                            ptxt = btn.find_element(By.CSS_SELECTOR, "div.Io6YTe").text.strip()
                            pmatch = re.search(r"\b(?:0\d(?: \d{2}){4}|\+33 ?\d(?: ?\d{2}){4}|0\d{9})\b", ptxt)
                            if pmatch:
                                data["phone"] = pmatch.group(0)
                        except: pass
                
                elif item_id == "authority" and not data["website"]:
                    href = btn.get_attribute("href")
                    if href and href.startswith("http"):
                        data["website"] = href

            ratings = self._extract_ratings(driver)
            data["averageRate"] = ratings["averageRate"]
            data["numberOfRate"] = ratings["numberOfRate"]

        except Exception as e:
            logger.error(f"  Error extracting from popup: {e}")

        return data

    def _extract_ratings(self, driver):
        data = {"averageRate": None, "numberOfRate": None}
        try:
            try:
                WebDriverWait(driver, 1).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div.F7nice"))
                )
            except Exception:
                pass

            # Extraction de la note (averageRate)
            try:
                f7nices = driver.find_elements(By.CSS_SELECTOR, "div.F7nice")
                for f7nice in f7nices:
                    html = f7nice.get_attribute("innerHTML") or ""
                    m_rate = re.search(r'([0-5],[0-9]|[0-5]\.[0-9])', html)
                    if m_rate:
                        data["averageRate"] = float(m_rate.group(1).replace(',', '.'))
                        break
            except Exception:
                pass

            # Extraction du nombre d'avis via le XPath fourni par l'utilisateur
            try:
                review_span = WebDriverWait(driver, 2).until(
                    EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'F7nice')]//span[@role='img' and contains(@aria-label,'avis')]"))
                )
                # Le text brut est souvent "(1 251)", on nettoie tout ce qui n'est pas un chiffre
                txt = review_span.text.strip()
                if not txt:
                    # En fallback, l'aria-label contient "1 251 avis"
                    txt = review_span.get_attribute("aria-label") or ""
                
                num_str = re.sub(r'[^\d]', '', txt)
                if num_str:
                    data["numberOfRate"] = int(num_str)
            except Exception:
                pass
            
        except Exception as e:
            logger.debug(f"    [ratings] global error: {e}")
        
        return data

    def _same_host(self, base, link):
        try:
            bu = urlparse(base)
            lu = urlparse(link)
            return bu.netloc == lu.netloc or (not lu.netloc)
        except Exception:
            return False

    def _close_popup(self, driver):
        """Ferme le panneau detail et attend le retour a la liste"""
        closed = False
        try:
            close_btns = driver.find_elements(By.CSS_SELECTOR, "button[aria-label*='Retour'], button[aria-label*='Back'], button.VfPpkd-icon-LgbsSe[aria-label*='Fermer']")
            if close_btns:
                close_btns[0].click()
                closed = True
        except Exception:
            pass

        if not closed:
            try:
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
                closed = True
            except Exception:
                pass

        if closed:
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
                )
                time.sleep(0.5)
            except Exception:
                time.sleep(1)

        return closed

    def _extract_socials(self, html):
        socials = {"facebookUrl": "", "xUrl": "", "linkedinUrl": "", "instagramUrl": ""}
        try:
            for href in re.findall(r'href=["\']([^"\']+)["\']', html or ""):
                u = href.lower()
                if not socials["facebookUrl"] and "facebook.com" in u:
                    socials["facebookUrl"] = href
                if not socials["xUrl"] and ("x.com" in u or "twitter.com" in u):
                    socials["xUrl"] = href
                if not socials["linkedinUrl"] and "linkedin.com" in u:
                    socials["linkedinUrl"] = href
                if not socials["instagramUrl"] and "instagram.com" in u:
                    socials["instagramUrl"] = href
        except Exception:
            pass
        return socials

    def _find_email_and_socials(self, website):
        email_regex = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        email = None
        socials = {"facebookUrl": "", "xUrl": "", "linkedinUrl": "", "instagramUrl": ""}
        html_main = ""

        try:
            resp = requests.get(website, headers=headers, timeout=5, allow_redirects=True)
            if resp.ok and resp.text:
                html_main = resp.text
                m = email_regex.search(resp.text)
                if m:
                    email = m.group(0)
                socials = self._extract_socials(resp.text)
        except Exception:
            pass

        if email:
            return email, socials

        candidate_paths = ["/contact", "/contactez-nous", "/contacts", "/mentions-legales", "/mentions", "/cgv", "/cgu", "/politique-de-confidentialite", "/privacy", "/about", "/a-propos"]
        links = []

        try:
            footers = re.findall(r"<footer[\s\S]*?</footer>", html_main, flags=re.IGNORECASE)
            search_zone = " ".join(footers) if footers else html_main

            for href in re.findall(r'href=["\']([^"\']*)["\']', search_zone):
                try:
                    u = href.strip()
                    if any(key in u.lower() for key in ("contact", "mention", "legal", "privacy", "confidential", "about", "propos")):
                        absu = urljoin(website, u)
                        if self._same_host(website, absu):
                            links.append(absu)
                except Exception:
                    continue
        except Exception:
            pass

        for p in candidate_paths:
            links.append(urljoin(website, p))

        seen = set()
        uniq = []
        for u in links:
            if u not in seen:
                seen.add(u)
                uniq.append(u)

        for u in uniq[:3]:
            try:
                r2 = requests.get(u, headers=headers, timeout=5, allow_redirects=True)
                if r2.ok and r2.text:
                    m2 = email_regex.search(r2.text)
                    if m2:
                        email = m2.group(0)

                    soc2 = self._extract_socials(r2.text)
                    for k, v in soc2.items():
                        if not socials.get(k) and v:
                            socials[k] = v

                    if email:
                        return email, socials
            except Exception:
                continue

        return email, socials

    def _post_lead(self, lead_data, city_id, niche):
        if not self.supabase:
            return

        payload = {
            "company": (lead_data.get("name") or "").strip(),
            "niche": niche.strip(),
            "address": (lead_data.get("address") or "").strip(),
        }

        if city_id:
            payload["cityId"] = str(city_id)

        if lead_data.get("phone"):
            payload["phone"] = lead_data["phone"].strip()

        if lead_data.get("averageRate") is not None:
            try:
                payload["averageRate"] = float(lead_data["averageRate"])
            except Exception:
                pass

        if lead_data.get("numberOfRate") is not None:
            try:
                payload["numberOfRate"] = int(lead_data["numberOfRate"])
            except Exception:
                pass

        if lead_data.get("website") and lead_data["website"].startswith("http"):
            payload["webSiteUrl"] = lead_data["website"]

        try:
            result = self.supabase.create_lead(payload)
            if isinstance(result, dict) and result.get("status") == "duplicate":
                logger.info(f"    Lead deja existant (doublon Supabase): {payload.get('company')}")
                return "duplicate"
            else:
                logger.info(f"    Lead pousse dans Supabase: {payload.get('company')}")
                return "success"
        except Exception as e:
            logger.error(f"    Lead push failed: {e}")
            return "error"

    def _update_lead_enrichment(self, lead_data, email, socials):
        if not self.supabase:
            return

        update_data = {}
        if email:
            update_data["email"] = email.strip()

        if socials:
            if socials.get("facebookUrl"):
                update_data["facebook_url"] = socials["facebookUrl"]
            if socials.get("xUrl"):
                update_data["x_url"] = socials["xUrl"]
            if socials.get("linkedinUrl"):
                update_data["linkedin_url"] = socials["linkedinUrl"]
            if socials.get("instagramUrl"):
                update_data["instagram_url"] = socials["instagramUrl"]

        if not update_data:
            return

        phone = lead_data.get("phone")
        company = lead_data.get("name")

        try:
            query_params = {}
            if phone:
                query_params["phone"] = phone
            elif company:
                query_params["company"] = company
            else:
                return

            success = self.supabase.update_lead(query_params, update_data)
            if not success:
                logger.warning(f"    Failed to update enrichment for {company}")
        except Exception as e:
            logger.warning(f"    Failed to update enrichment: {e}")

    def _mark_done(self, city_id, niche):
        try:
            data = {}
            if os.path.exists(self.history_path):
                with open(self.history_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

            city_key = str(city_id)
            if city_key not in data:
                data[city_key] = []
            if niche not in data[city_key]:
                data[city_key].append(niche)

            with open(self.history_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"  Could not mark {city_id}/{niche} as done: {e}")

    def _recycle_driver_if_needed(self):
        """Ferme et recrée le driver tous les N villes pour éviter les fuites mémoire."""
        if self.cities_scraped >= self.max_cities_per_driver:
            logger.info(f"  Recyclage du driver (après {self.cities_scraped} villes)...")
            self.close()
            self.driver = None
            self.cities_scraped = 0
            time.sleep(2)  # Pause pour libérer les ressources

    def scrape(self, city_name, city_id, niche, on_lead_enriched=None):
        """Scrape une ville pour une niche donnee.

        Args:
            on_lead_enriched: callback(lead_data, email, socials) appele quand un lead est enrichi avec email/socials.
        """
        import urllib.parse
        query = f"{niche} {city_name}"
        url = f"https://www.google.com/maps/search/{urllib.parse.quote_plus(query)}"

        logger.info(f"  URL: {url}")

        try:
            # Recycler le driver si nécessaire
            self._recycle_driver_if_needed()

            if not self.driver:
                logger.info("  Initialisation du navigateur...")
                self.driver = self._build_driver()

            logger.info("  Chargement de Google Maps...")
            try:
                self.driver.get(url)
            except Exception as e:
                logger.warning(f"  Timeout ou erreur chargement page: {e}, recyclage du driver...")
                self.close()
                self.driver = self._build_driver()
                self.driver.get(url)
            self._accept_consent(self.driver)

            logger.info(f"  Scroll pour charger toutes les cards...")
            self._scroll_feed(self.driver)

            cards = self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
            total_cards = len(cards)
            logger.info(f"  {total_cards} cards trouvees")

            leads_posted = 0
            previous_name = None

            for idx in range(total_cards):
                try:
                    cards = self.driver.find_elements(By.CSS_SELECTOR, "div.Nv2PK")
                    if idx >= len(cards):
                        continue

                    card = cards[idx]

                    # Nom attendu depuis aria-label de la card
                    expected_name = card.get_attribute("aria-label") or ""
                    expected_name = expected_name.split("\u00b7")[0].strip()

                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                    time.sleep(0.3)

                    link = card.find_element(By.CSS_SELECTOR, "a.hfpxzc")
                    self.driver.execute_script("arguments[0].click();", link)

                    # Attendre que le panneau detail affiche un nom different
                    if previous_name:
                        try:
                            def _panel_changed(d):
                                elems = d.find_elements(By.CSS_SELECTOR, "h1.DUwDvf")
                                if not elems:
                                    return False  # panneau en transition, attendre
                                txt = elems[0].text.strip()
                                return txt and txt != previous_name
                            WebDriverWait(self.driver, 8).until(_panel_changed)
                        except Exception:
                            pass

                    lead_data = self._extract_card_details(self.driver, expected_name=expected_name)

                    # Fallback nom depuis la card
                    if not lead_data.get("name") and expected_name:
                        lead_data["name"] = expected_name

                    # Detection doublon
                    current_name = lead_data.get("name")
                    if current_name and current_name == previous_name:
                        logger.warning(f"  [{idx+1}/{total_cards}] DOUBLON: {current_name} -> skip")
                        self._close_popup(self.driver)
                        continue

                    # --- LOG DETAILLE PAR CARD ---
                    phone = lead_data.get("phone") or "-"
                    site = lead_data.get("website") or "-"
                    addr = lead_data.get("address") or "-"
                    rate = lead_data.get("averageRate")
                    nb_rate = lead_data.get("numberOfRate")
                    rate_str = f"{rate}/5 ({nb_rate or '?'} avis)" if rate else "-"

                    logger.info(f"  [{idx+1}/{total_cards}] {current_name or '???'}")
                    logger.info(f"    Tel: {phone} | Site: {site}")
                    logger.info(f"    Adresse: {addr}")
                    logger.info(f"    Note: {rate_str}")

                    if any(lead_data.values()):
                        website = lead_data.get("website")

                        push_status = self._post_lead(lead_data, city_id, niche)
                        leads_posted += 1
                        previous_name = current_name

                        # Même si c'est un doublon Supabase, on veut essayer de l'enrichir et l'envoyer à Brevo
                        # si on n'a pas pu le faire avant (par exemple si le job a crashé ou si on relance pour enrichir)
                        # Enrichissement email + reseaux sociaux
                        if website:
                            try:
                                email, socials = self._find_email_and_socials(website)
                                email_str = email or "-"
                                social_found = [k for k, v in socials.items() if v]
                                logger.info(f"    Email: {email_str} | Socials: {', '.join(social_found) if social_found else '-'}")
                                if email or any(socials.values()):
                                    self._update_lead_enrichment(lead_data, email, socials)
                                # Callback pour envoi direct Brevo si email trouve
                                if on_lead_enriched:
                                    try:
                                        on_lead_enriched(lead_data, email, socials)
                                    except Exception as cb_err:
                                        logger.debug(f"    Callback error: {cb_err}")
                            except Exception as e:
                                logger.debug(f"    Enrichment error: {e}")
                        else:
                            logger.info("    Email: - (pas de site web)")
                            if on_lead_enriched:
                                try:
                                    on_lead_enriched(lead_data, None, None)
                                except Exception as cb_err:
                                    logger.debug(f"    Callback error: {cb_err}")
                    else:
                        logger.warning(f"  [{idx+1}/{total_cards}] Aucune donnee extraite, skip")

                    self._close_popup(self.driver)

                except Exception as e:
                    logger.error(f"  [{idx+1}/{total_cards}] ERREUR: {e}")
                    self._close_popup(self.driver)
                    continue

            logger.info(f"  RESULTAT: {leads_posted}/{total_cards} leads extraits pour {city_name}")
            self._mark_done(city_id, niche)
            self.cities_scraped += 1  # Incrémenter le compteur
            return True

        except Exception as e:
            logger.error(f"  Erreur scrape {city_name}: {e}")
            # En cas d'erreur Chromium, recycler le driver
            if "unknown error" in str(e).lower() or "crashed" in str(e).lower():
                logger.warning(f"  Erreur Chromium détectée, recyclage du driver...")
                self.close()
                self.driver = None
                self.cities_scraped = 0
            return False

    def close(self):
        if self.driver:
            try:
                logger.info("Fermeture du navigateur...")
                self.driver.quit()
                self.driver = None
            except Exception as e:
                logger.warning(f"Erreur fermeture: {e}")
