#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, re, time, datetime, sys
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.getenv("MAX_LISTINGS", "10"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", "10"))
PROXY       = os.getenv("PROXY", "").strip()

CSV_PATH = "airbnb_results.csv"
HEADERS = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]

# ---------- helpers ----------

def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def accept_cookies(page):
    selectors = [
        "button:has-text('Accept')",
        "button:has-text('Accepter')",
        "button:has-text('OK')",
        "button[aria-label*='Accept']",
        "button[aria-label*='Accepter']",
    ]
    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=1500)
            page.wait_for_timeout(300)
            break
        except Exception:
            pass

def page_text(page):
    # robuste et simple
    try:
        return page.inner_text("body", timeout=10000)
    except Exception:
        return page.content()

def first_or_blank(items):
    return items[0] if items else ""

# ---------- URL collection ----------

def collect_listing_urls(page, start_url, max_n, max_minutes):
    print(f"START {start_url}")
    page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
    accept_cookies(page)

    seen, stagnant = set(), 0
    t0 = time.time()
    while len(seen) < max_n and (time.time() - t0) < max_minutes * 60:
        # scroll fort pour charger
        page.mouse.wheel(0, 20000)
        page.wait_for_timeout(800)
        # récupérer tous les liens /rooms/
        hrefs = page.eval_on_selector_all(
            "a[href*='/rooms/']",
            "els => Array.from(new Set(els.map(a => new URL(a.getAttribute('href'), location.origin).href.split('?')[0])))"
        )
        before = len(seen)
        for h in hrefs:
            if "/rooms/" in h:
                seen.add(h)
        stagnant = stagnant + 1 if len(seen) == before else 0
        if stagnant >= 5:
            break
    return list(seen)[:max_n]

# ---------- Host profile scraping ----------

JOIN_PATTERNS = [
    r"Joined in\s+(\d{4})",
    r"Membre depuis\s+(\d{4})",
    r"S'est inscrit en\s+(\d{4})",
    r"Inscrit en\s+(\d{4})",
]

def parse_join_year(text):
    for pat in JOIN_PATTERNS:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return ""

def fetch_host_details(browser, profile_url):
    if not profile_url:
        return {"host_name":"", "host_joined":""}
    p = browser.new_page()
    try:
        p.goto(profile_url, wait_until="domcontentloaded", timeout=45000)
        accept_cookies(p)
        # nom sur profil: souvent h1 ou [data-testid='user-profile-name']
        name = ""
        try:
            name = p.locator("[data-testid='user-profile-name']").first.inner_text(timeout=2000).strip()
        except Exception:
            try:
                name = p.locator("h1").first.inner_text(timeout=2000).strip()
            except Exception:
                name = ""
        text = page_text(p)
        joined = parse_join_year(text)
        return {"host_name": name, "host_joined": joined}
    except Exception:
        return {"host_name":"", "host_joined":""}
    finally:
        p.close()

# ---------- Listing scraping ----------

LIC_PATTERNS = [
    r"(?:license|licen[cs]e|permit|permis|tourism|dtcm)[^A-Za-z0-9]{0,20}([A-Z0-9][A-Z0-9\-\/]{4,})",
]

def extract_license(text):
    for pat in LIC_PATTERNS:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip(" .,:;·")
    return ""

def extract_rating(text):
    # ex: "4.83 · 120 reviews" ou "4,83 · 120 avis"
    m = re.search(r"\b([0-5](?:[.,]\d{1,2})?)\s*[·•]\s*\d+\s*(?:reviews|avis)", text, flags=re.IGNORECASE)
    if m:
        return m.group(1).replace(",", ".")
    # fallback aria-label style "Rated 4.9 out of 5"
    m = re.search(r"Rated\s+([0-5](?:[.,]\d{1,2})?)\s+out of 5", text, flags=re.IGNORECASE)
    return m.group(1).replace(",", ".") if m else ""

def find_host_profile_url(page):
    try:
        urls = page.eval_on_selector_all(
            "a[href*='/users/']",
            "els => els.map(a => a.href)"
        )
        # privilégier /users/show/
        for u in urls:
            if "/users/show/" in u:
                return u.split("?")[0]
        return first_or_blank(urls).split("?")[0] if urls else ""
    except Exception:
        return ""

def extract_host_name_from_listing(text):
    m = re.search(r"(?:Hosted by|Hôte\s*:)\s*([A-Za-zÀ-ÖØ-öø-ÿ'’\-\.\s]{2,40})", text)
    return m.group(1).strip() if m else ""

def scrape_listing(browser, url):
    page = browser.new_page()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        accept_cookies(page)

        # titre
        try:
            title = page.locator("h1").first.inner_text(timeout=5000).strip()
        except Exception:
            title = ""

        text = page_text(page)
        license_code = extract_license(text)
        rating = extract_rating(text)

        host_profile_url = find_host_profile_url(page)
        host_name = extract_host_name_from_listing(text)

        # compléter via profil si besoin
        if host_profile_url:
            details = fetch_host_details(browser, host_profile_url)
            host_name = details["host_name"] or host_name
            host_joined = details["host_joined"]
        else:
            host_joined = ""

        return {
            "url": url,
            "title": title,
            "license_code": license_code,
            "host_name": host_name,
            "host_overall_rating": rating,
            "host_profile_url": host_profile_url,
            "host_joined": host_joined,
            "scraped_at": now_iso(),
        }
    except Exception:
        return {
            "url": url,
            "title": "",
            "license_code": "",
            "host_name": "",
            "host_overall_rating": "",
            "host_profile_url": "",
            "host_joined": "",
            "scraped_at": now_iso(),
        }
    finally:
        try:
            page.close()
        except Exception:
            pass

# ---------- main ----------

def read_urls_txt():
    if not os.path.exists("urls.txt"):
        return []
    with open("urls.txt", "r", encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]

def write_csv(rows):
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADERS)
        w.writeheader()
        for r in rows:
            # s'assurer que toutes les clés existent
            for k in HEADERS:
                r.setdefault(k, "")
            w.writerow(r)

def run():
    urls = []

    with sync_playwright() as p:
        launch_args = dict(headless=True, args=["--disable-blink-features=AutomationControlled"])
        if PROXY:
            launch_args["proxy"] = {"server": PROXY}
        browser = p.chromium.launch(**launch_args)

        context = browser.new_context(user_agent=None, viewport={"width": 1366, "height": 900})
        page = context.new_page()

        try:
            urls = collect_listing_urls(page, START_URL, MAX_LIST, MAX_MINUTES)
        except Exception as e:
            print(f"Failed to collect URLs: {e}")

        # fallback urls.txt
        if not urls:
            txt_urls = read_urls_txt()
            if txt_urls:
                urls = txt_urls[:MAX_LIST]

        print(f"FOUND_URLS {len(urls)}")

        rows = []
        for i, u in enumerate(urls, 1):
            try:
                row = scrape_listing(browser, u)
                rows.append(row)
            except Exception as e:
                rows.append({
                    "url": u, "title":"", "license_code":"", "host_name":"",
                    "host_overall_rating":"", "host_profile_url":"", "host_joined":"", "scraped_at": now_iso()
                })
            # petite pause anti-bot
            time.sleep(1.0)

        write_csv(rows)
        print(f"SAVED {len(rows)} rows to {CSV_PATH}")

        try:
            browser.close()
        except Exception:
            pass

if __name__ == "__main__":
    run()
