
import time, re
from bs4 import BeautifulSoup
import json, requests
import concurrent.futures as futures
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from playwright.sync_api import sync_playwright
import time
import re
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
import time, threading, random
import json, math, threading, re, random, time as _t
from collections import defaultdict
from concurrent import futures

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/115.0.0.0 Safari/537.36")
}






def scrape_zomato_links(city="bangalore", area=None, no_of_restaurants=1000):
    city = city.strip().lower().replace(" ", "-")
    area = area.strip().lower().replace(" ", "-") if area else None

    if area:
        location_path = f"{city}/{area}-restaurants"
    else:
        location_path = f"{city}/"

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    category_ids = [1,2, 3]
    if area==None :
        category_ids = ["dine-out", "drinks-and-nightlife","order-food-online" ]
    
    all_links = set()

    for category_id in category_ids:
        
        base_url = f"https://www.zomato.com/{location_path}"
        # base_url += f"?category={category_id}" if category_id else ""
        if area is None:
            base_url += category_id
        else:
            base_url += f"?category={category_id}"
        print(f"\n[INFO] Starting scrape: {base_url}")
        driver.get(base_url)

        driver.execute_script("window.scrollTo(0, 300)")
        time.sleep(3)

        wait = WebDriverWait(driver, 20)
        try:
            # For delivery category, wait for any React block
            if category_id == 1:
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='sc-']")))
                time.sleep(5)

            # Then wait for links
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "a[href]")))
        except:
            print(f"[WARN] Retry: Waiting extra for {base_url}")
            time.sleep(7)
            soup_retry = BeautifulSoup(driver.page_source, 'html.parser')
            if not soup_retry.find_all('a', href=True):
                print(f"[ERROR] Still no cards loaded. Skipping: {base_url}")
                continue

           
        
        restaurant_links = set()
        repeat_count = 0
        max_repeat_limit = 3

        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(5)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            links_on_page = set()
            for tag in soup.find_all('a', href=True):
                href = tag['href']
                # if category_id is 1:
                #     print(href)
                if not href.startswith("/"):
                    continue

                # Category-aware link filtering
                if category_id == 1 or category_id == "order-food-online":
                    if "/order" in href or "/menu" in href or "/restaurant" in href or "/info" in href :
                        links_on_page.add("https://www.zomato.com" + href)
                else:
                    if f"/{city}/" in href and "/info" in href:
                        links_on_page.add("https://www.zomato.com" + href)

            old_count = len(restaurant_links)
            restaurant_links.update(links_on_page)
            new_count = len(restaurant_links)

            print(f"[CATEGORY {category_id or 'default'}] Collected: {new_count}")

            if new_count >= no_of_restaurants:
                print("[✅] Target reached in this category.")
                break

            if new_count == old_count:
                repeat_count += 1
            else:
                repeat_count = 0

            if repeat_count >= max_repeat_limit:
                print("[⚠️] No new links found after scrolling. Moving to next category.")
                break

        all_links.update(restaurant_links)
        if len(all_links) >= no_of_restaurants:
            print("[✅] Target reached .")
            break 
    print(f"[INFO] Total unique links collected: {len(all_links)}")
    driver.quit()
    return list(all_links)[:no_of_restaurants]

RESTAURANT_TYPES = {
    "Restaurant", "LocalBusiness", "FoodEstablishment",
    "CafeOrCoffeeShop", "Bakery", "BarOrPub", "FastFoodRestaurant",
    "IceCreamShop", "PizzaRestaurant", "Brewery", "Winery", "Distillery",
    "DairyStore"
}

def _jsonld_pick_restaurant(obj):
    if isinstance(obj, list):
        for it in obj:
            hit = _jsonld_pick_restaurant(it)
            if hit: return hit
        return None
    if not isinstance(obj, dict):
        return None
    if "@graph" in obj and isinstance(obj["@graph"], list):
        for it in obj["@graph"]:
            hit = _jsonld_pick_restaurant(it)
            if hit: return hit
    typ = obj.get("@type")
    if isinstance(typ, str) and typ in RESTAURANT_TYPES: return obj
    if isinstance(typ, list) and any(isinstance(t, str) and t in RESTAURANT_TYPES for t in typ): return obj
    return None

def _addr_text(addr):
    if isinstance(addr, dict):
        return (addr.get("streetAddress")
                or addr.get("addressLocality")
                or addr.get("addressRegion")
                or addr.get("address"))
    if isinstance(addr, list):
        for a in addr:
            if isinstance(a, dict):
                s = (a.get("streetAddress")
                     or a.get("addressLocality")
                     or a.get("addressRegion")
                     or a.get("address"))
                if s: return s
    if isinstance(addr, str):
        return addr
    return None

def _phone_text(val):
    if isinstance(val, list):
        return ", ".join([str(v).strip() for v in val if v])
    return (str(val).strip() if val else "NA")

def recover_no_ldjson_with_selenium(urls):
    """
    Visit each URL, try to read JSON-LD from the rendered DOM; if still missing,
    fall back to grabbing name/phone/address from visible elements.
    Returns list of [Name, Address, Phone] rows for ones we can recover.
    """
    if not urls:
        return []

    opts = ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1280,900")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    # block some heavy assets
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
    recovered = []

    try:
        for u in urls:
            try:
                driver.get(u)
                time.sleep(0.8)  # small settle

                html = driver.page_source
                soup = BeautifulSoup(html, "lxml")

                # 1) Try JSON-LD from rendered DOM
                data = None
                for sc in soup.find_all("script", type="application/ld+json"):
                    try:
                        obj = json.loads(sc.string or "")
                    except Exception:
                        continue
                    hit = _jsonld_pick_restaurant(obj)
                    if hit:
                        data = hit
                        break

                if data:
                    name = data.get("name")
                    addr = _addr_text(data.get("address"))
                    phone = _phone_text(data.get("telephone"))
                    recovered.append([name, addr, phone])
                    continue

                # 2) DOM fallback (no JSON-LD)
                # name: h1 or og:title
                h1 = soup.find("h1")
                name = h1.get_text(strip=True) if h1 else None
                if not name:
                    og = soup.find("meta", attrs={"property": "og:title"})
                    if og and og.get("content"): name = og["content"].strip()

                # phone: first tel: link
                phone = None
                a_tel = soup.find("a", href=re.compile(r"^tel:", re.I))
                if a_tel:
                    phone = re.sub(r"^tel:\s*", "", a_tel.get("href") or "", flags=re.I).strip()
                    if not phone:
                        phone = a_tel.get_text(strip=True) or None
                phone = phone or "NA"

                # address: common selectors/heuristics
                addr = None
                candidates = [
                    '[role="address"]','address','[class*="address"]','[class*="Address"]',
                    '[data-testid*="address"]'
                ]
                for sel in candidates:
                    el = soup.select_one(sel)
                    if el:
                        txt = el.get_text(" ", strip=True)
                        if txt and len(txt) > 5:
                            addr = txt
                            break
                if not addr:
                    # last-chance: element whose aria-label/title/class mentions address
                    for el in soup.find_all(True):
                        lab = (el.get("aria-label") or "") + " " + (el.get("title") or "")
                        cls = " ".join(el.get("class") or [])
                        if re.search(r"address", lab, re.I) or re.search(r"address", cls, re.I):
                            txt = el.get_text(" ", strip=True)
                            if txt and len(txt) > 5:
                                addr = txt
                                break

                if name or addr or phone:
                    recovered.append([name, addr, phone])
            except Exception:
                # skip this URL if DOM fallback also fails
                continue
    finally:
        driver.quit()

    return recovered



# -------------------------
# Robust LD+JSON extraction
# -------------------------
RESTAURANT_TYPES = {
    "Restaurant", "LocalBusiness", "FoodEstablishment",
    "CafeOrCoffeeShop", "Bakery", "BarOrPub", "FastFoodRestaurant",
    "IceCreamShop", "PizzaRestaurant", "Brewery", "Winery", "Distillery",
    "DairyStore"
}

def _first_restaurant_from_jsonld(obj):
    # obj can be dict or list; handle @graph and type as list/str
    if isinstance(obj, list):
        for it in obj:
            hit = _first_restaurant_from_jsonld(it)
            if hit:
                return hit
        return None
    if not isinstance(obj, dict):
        return None
    if "@graph" in obj and isinstance(obj["@graph"], list):
        for it in obj["@graph"]:
            hit = _first_restaurant_from_jsonld(it)
            if hit:
                return hit
    typ = obj.get("@type")
    if isinstance(typ, str) and typ in RESTAURANT_TYPES:
        return obj
    if isinstance(typ, list) and any(isinstance(t, str) and t in RESTAURANT_TYPES for t in typ):
        return obj
    return None

def _extract_ldjson(soup: BeautifulSoup):
    """Return first JSON-LD blob that looks like a restaurant entity."""
    for sc in soup.find_all('script', type='application/ld+json'):
        try:
            data = json.loads(sc.string or "")
        except Exception:
            continue
        hit = _first_restaurant_from_jsonld(data)
        if hit:
            return hit
    return None



HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/115.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}
UA_POOL = [
    # Desktop
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    # Mobile (some pages return cleaner schema)
    "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.70 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
]


class RateLimiter:
    """
    Simple global limiter: at most `rate_per_sec` requests per second (burst-limited).
    """
    def __init__(self, rate_per_sec: float = 8.0):
        self.min_interval = 1.0 / rate_per_sec
        self.lock = threading.Lock()
        self._next_ok = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            if now < self._next_ok:
                time.sleep(self._next_ok - now)
                now = time.monotonic()
            # jitter helps avoid request “bursts”
            jitter = random.uniform(0.0, 0.05)
            self._next_ok = now + self.min_interval + jitter

GLOBAL_LIMITER = RateLimiter(rate_per_sec=8.0)  # tune: 6–10 is gentle



def _get_with_limit(session, url, timeout=25):
    # global pacing + explicit respect for Retry-After when 429
    for attempt in range(1, 5):  # up to 4 tries (first + 3 retries)
        GLOBAL_LIMITER.wait()
        resp = session.get(url, timeout=timeout)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                sleep_s = float(ra) if ra else min(2 ** attempt, 8)  # 2,4,8 fallback
            except Exception:
                sleep_s = min(2 ** attempt, 8)
            time.sleep(sleep_s + random.uniform(0.05, 0.15))
            continue
        return resp
    return resp  # return last response anyway


def _make_session(user_agent: str = None):
    s = requests.Session()
    retry = Retry(
        total=3, connect=3, read=3,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,   # <-- important for 429
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": user_agent or HEADERS["User-Agent"] or UA_POOL[0],
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.google.com/",
    })
    return s



def get_info_verbose(url, session: requests.Session):
    try:
        resp = _get_with_limit(session, url, timeout=25)
        if resp.status_code != 200:
            return None, f"status_{resp.status_code}"

        html = resp.text
        low = html.lower()
        if "captcha" in low or "are you a human" in low:
            return None, "captcha"

        soup = BeautifulSoup(html, "lxml")
        data = _extract_ldjson(soup)
        if not data:
            return None, "no_ldjson"

        addr = data.get("address") or {}
        if isinstance(addr, dict):
            street = addr.get("streetAddress")
        elif isinstance(addr, list) and addr and isinstance(addr[0], dict):
            street = addr[0].get("streetAddress")
        elif isinstance(addr, str):
            street = addr
        else:
            street = None

        phone = data.get("telephone") or "NA"
        return {
            "Name": data.get("name"),
            "Address": street,
            "Phone": str(phone).strip()
        }, "ok"
    except requests.exceptions.Timeout:
        return None, "timeout"
    except Exception as e:
        return None, f"exc_{type(e).__name__}"



def retry_info(url, session: requests.Session):
    # try mobile UA and /info rewrite on a fresh session
    mobile_ua = "Mozilla/5.0 (Linux; Android 12; Pixel 5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.70 Mobile Safari/537.36"
    s2 = _make_session(user_agent=mobile_ua) if "_make_session" in globals() else session

    # normalize variants
    v1 = url
    v2 = re.sub(r"(/[^/]+)/(order|menu|delivery|restaurant)(\?.*)?$", r"\1/info", url)
    variants = [v for v in [v1, v2] if v]

    for v in variants:
        try:
            r = s2.get(v, timeout=30)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "lxml")
            data = _extract_ldjson(soup)
            if not data:
                continue
            addr = data.get("address") or {}
            if isinstance(addr, dict):
                street = addr.get("streetAddress")
            elif isinstance(addr, list) and addr and isinstance(addr[0], dict):
                street = addr[0].get("streetAddress")
            elif isinstance(addr, str):
                street = addr
            else:
                street = None
            phone = data.get("telephone") or "NA"
            return {"Name": data.get("name"), "Address": street, "Phone": str(phone).strip()}
        except Exception:
            continue
    return None


# -------------------------
# Chunking + per-thread work
# -------------------------
def _split_into_chunks(urls, n_chunks):
    size = math.ceil(len(urls) / n_chunks)
    return [urls[i:i+size] for i in range(0, len(urls), size)]





def get_restaurant_info_chunked(url_list, no_of_restaurants, max_workers=20):
    urls = url_list[:no_of_restaurants]
    if not urls:
        return []

    n_threads = max(1, min(max_workers, len(urls)))
    chunks = _split_into_chunks(urls, n_threads)

    from collections import Counter

    def worker(chunk):
        session = _make_session()
        ok, fail = [], []
        reasons = []

        for u in chunk:
            info, reason = get_info_verbose(u, session=session)
            if info:
                ok.append([info["Name"], info["Address"], info["Phone"]])
            else:
                fail.append(u)
                reasons.append(reason)

        # Second pass only on fails
        for u in fail[:]:
            info = retry_info(u, session=session)
            if info:
                ok.append([info["Name"], info["Address"], info["Phone"]])
                # remove from fail and its reason record
                idx = fail.index(u)
                fail.pop(idx)
                reasons.pop(idx)

        print(f"[THREAD {threading.current_thread().name}] saved={len(ok)} rem_fail={len(fail)}")
        # Return both results and reason counts for visibility
        return ok, Counter(reasons), list(zip(fail, reasons))


    with futures.ThreadPoolExecutor(max_workers=n_threads) as ex:
        parts = list(ex.map(worker, chunks))

    results, reason_counts = [], Counter()
    failed_pairs_all = []  # <-- collect (url, reason)
    for ok, rc, failed_pairs in parts:  # <-- unpack the 3-tuple
        results.extend(ok)
        reason_counts.update(rc)
        failed_pairs_all.extend(failed_pairs)
    
    no_ldjson_urls = [u for (u, reason) in failed_pairs_all if reason == "no_ldjson"]

    if no_ldjson_urls:
        recovered_rows = recover_no_ldjson_with_selenium(no_ldjson_urls)
        results.extend(recovered_rows)
        # also reduce the failure count you report
        for _ in range(len(recovered_rows)):
            # decrement one 'no_ldjson' from reason_counts if present
            if reason_counts.get("no_ldjson", 0) > 0:
                reason_counts["no_ldjson"] -= 1

    # de-dup by (Name, Phone)
    seen, deduped = set(), []
    for name, addr, phone in results:
        key = ((name or "").strip(), (phone or "").strip())
        if key in seen:
            continue
        seen.add(key)
        deduped.append([name, addr, phone])

    print(f"[SUMMARY] saved={len(deduped)} failed={sum(reason_counts.values())} (threads={n_threads})")
    if reason_counts:
        print("[FAIL REASONS]", dict(reason_counts))
    return deduped



def scrapper(city, area, no_of_restaurants):
    urls = scrape_zomato_links(city, area, no_of_restaurants)
    # Tune workers: 32–48 is a good start. If you hit throttling, lower it.
    return get_restaurant_info_chunked(urls, no_of_restaurants ,max_workers=20)
