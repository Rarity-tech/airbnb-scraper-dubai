# scrape_airbnb.py
import os, re, csv, time, math
from datetime import datetime, timezone
from urllib.parse import urljoin

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "10"))
MAX_MINUTES  = int(os.getenv("MAX_MINUTES",  "10"))
PROXY = os.getenv("PROXY", "").strip()

HEADERS = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]

MONTHS = {
    "janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"decembre":12,
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,"july":7,"august":8,"september":9,"october":10,"november":11,"december":12
}

# ---------- helpers ----------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def clean(s):
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def safe_click(page, locator):
    try:
        page.locator(locator).first.click(timeout=2500)
    except Exception:
        pass

def accept_cookies(page):
    # tries FR/EN
    for name in [r"Accepter|Tout accepter|J’accepte|J'accepte|OK", r"Accept|Agree"]:
        try:
            page.get_by_role("button", name=re.compile(name, re.I)).first.click(timeout=2000)
            return
        except Exception:
            continue

def goto(page, url):
    try:
        page.goto(url, wait_until="load", timeout=40000)
    except PWTimeout:
        # try lighter wait
        page.goto(url, wait_until="domcontentloaded", timeout=40000)

# ---------- listing URL collection ----------
def collect_listing_urls(page, max_list, max_minutes):
    goto(page, START_URL)
    accept_cookies(page)
    deadline = time.time() + max_minutes*60
    seen = set()
    while len(seen) < max_list and time.time() < deadline:
        # collect visible room links
        hrefs = page.eval_on_selector_all(
            "a[href*='/rooms/']",
            "els => els.map(e => e.getAttribute('href'))"
        )
        for h in hrefs:
            if not h: 
                continue
            if "/experiences/" in h: 
                continue
            # normalize
            if h.startswith("http"):
                u = h
            else:
                u = urljoin(page.url, h)
            # keep only exact room pages
            if re.search(r"/rooms/\d+", u):
                seen.add(u.split("?")[0])
            if len(seen) >= max_list:
                break

        # scroll to load more
        page.mouse.wheel(0, 1600)
        page.wait_for_timeout(600)

    urls = list(seen)[:max_list]
    return urls

# ---------- data extractors ----------
def extract_title(page):
    # H1 or first heading
    for sel in ["h1", "header h1", "[data-testid='title']"]:
        try:
            t = clean(page.locator(sel).first.inner_text(timeout=2000))
            if t: return t
        except Exception:
            pass
    return ""

def open_about_modal_if_any(page):
    # Try to open "Lire la suite" / "Afficher plus" / "Read more"
    for text in [r"Lire la suite", r"Afficher plus", r"Read more", r"Show more"]:
        try:
            page.get_by_role("button", name=re.compile(text, re.I)).first.click(timeout=2000)
            page.wait_for_timeout(400)  # allow modal to render
            return
        except Exception:
            continue

def extract_license_code_from_text(text):
    if not text: 
        return ""
    # labels FR/EN
    LABELS = r"(Infos d.?enregistrement|D[ée]tails de l.?enregistrement|Num[ée]ro d.?enregistrement|Registration number|License|Licence|Permit)"
    # 1) label then code
    m = re.search(LABELS + r".{0,80}?([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}|\d{6,10})", text, re.I|re.S)
    if m:
        return clean(m.group(2))

    # 2) free-form patterns seen à Dubaï
    m = re.search(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", text)
    if m: return m.group(1)
    m = re.search(r"\b(\d{7,10})\b", text)  # ex: 1100042
    if m: return m.group(1)
    return ""

def extract_license_code(page):
    # search in whole page then modal
    try:
        txt = page.inner_text("body", timeout=2000)
        code = extract_license_code_from_text(txt)
        if code:
            return code
    except Exception:
        pass
    open_about_modal_if_any(page)
    try:
        txt = page.inner_text("body", timeout=2000)
        code = extract_license_code_from_text(txt)
        return code
    except Exception:
        return ""

def get_host_section(page):
    # Find the container that contains the host card only
    candidates = page.locator(
        ":is(section,div,main)"
        ).filter(has_text=re.compile(
            r"Faites connaissance avec votre h[ôo]te|Meet your host|Get to know your host", re.I
        ))
    try:
        return candidates.first
    except Exception:
        return None

def extract_host_core(page):
    """
    Returns name, profile_url, section_text
    Only from the host card to avoid reviewer links.
    """
    sec = get_host_section(page)
    name = ""
    profile = ""
    section_text = ""
    if sec:
        try:
            a = sec.locator("a[href^='/users/show/'], a[data-testid*='profile'][href^='/users/show/']").first
            profile_href = a.get_attribute("href", timeout=2000)
            name = clean(a.inner_text(timeout=2000))
            if profile_href:
                profile = urljoin(page.url, profile_href)
        except Exception:
            pass
        try:
            section_text = clean(sec.inner_text(timeout=2000))
        except Exception:
            section_text = ""
    return name, profile, section_text

def extract_host_rating(section_text):
    if not section_text: 
        return ""
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*[★⭐]", section_text)
    return m.group(1).replace(",", ".") if m else ""

def extract_host_joined(section_text):
    if not section_text:
        return ""

    # 1) "Hôte depuis <mois> <année>" or "Host since <month> <year>"
    m = re.search(r"H[oô]te depuis\s+([A-Za-zéûôîà]+)?\s*(\d{4})", section_text, re.I)
    if not m:
        m = re.search(r"Host since\s+([A-Za-z]+)?\s*(\d{4})", section_text, re.I)
    if m:
        month = (m.group(1) or "").lower()
        year  = m.group(2)
        if month and month in MONTHS:
            return f"{year}-{MONTHS[month]:02d}"
        return year

    # 2) "<n> ans sur Airbnb" -> approximate join year
    m = re.search(r"(\d+)\s+an[s]?\s+sur\s+Airbnb", section_text, re.I)
    if m:
        years = int(m.group(1))
        return str(datetime.now().year - years)

    # 3) "<n> years on Airbnb"
    m = re.search(r"(\d+)\s+year[s]?\s+on\s+Airbnb", section_text, re.I)
    if m:
        years = int(m.group(1))
        return str(datetime.now().year - years)

    return ""

def parse_listing(page, url):
    goto(page, url)
    accept_cookies(page)

    title = extract_title(page)
    license_code = extract_license_code(page)

    host_name, host_profile_url, host_block_text = extract_host_core(page)
    host_rating = extract_host_rating(host_block_text)
    host_joined = extract_host_joined(host_block_text)

    return {
        "url": url.split("?")[0],
        "title": title,
        "license_code": license_code,
        "host_name": host_name,
        "host_overall_rating": host_rating,
        "host_profile_url": host_profile_url,
        "host_joined": host_joined,
        "scraped_at": now_iso(),
    }

# ---------- main ----------
def main():
    print(f"START {START_URL}")
    with sync_playwright() as p:
        launch_args = {
            "headless": True,
            "args": ["--no-sandbox","--disable-dev-shm-usage"]
        }
        if PROXY:
            launch_args["proxy"] = {"server": PROXY}
        browser = p.chromium.launch(**launch_args)
        context = browser.new_context(
            locale="fr-FR",
            timezone_id="Asia/Dubai",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )
        page = context.new_page()

        try:
            urls = collect_listing_urls(page, MAX_LISTINGS, MAX_MINUTES)
        except Exception as e:
            print(f"Failed to collect URLs: {e}")
            urls = []

        print(f"FOUND_URLS {len(urls)}")
        for i, u in enumerate(urls, 1):
            print(f"#{i} {u}")

        rows = []
        for u in urls:
            try:
                rows.append(parse_listing(page, u))
            except Exception as e:
                print(f"ERROR parsing {u}: {e}")

        # write CSV with BOM for Excel
        with open("airbnb_results.csv", "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=HEADERS)
            w.writeheader()
            for r in rows:
                w.writerow(r)

        print(f"SAVED {len(rows)} rows to airbnb_results.csv")
        context.close()
        browser.close()

if __name__ == "__main__":
    main()
