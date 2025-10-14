# scrape_airbnb.py
# Extraction robuste du code d'enregistrement + hôte + note + lien profil
# Entrées via variables d'environnement:
#   START_URL, MAX_LISTINGS, MAX_MINUTES, PROXY
# Sortie: airbnb_results.csv avec colonnes:
#   url,title,license_code,host_name,host_overall_rating,host_profile_url,host_joined,price_text,scraped_at

import os
import re
import sys
import csv
import time
import math
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, Page, TimeoutError as PWTimeout

START_URL = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "50"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", "10"))
PROXY = os.getenv("PROXY", "").strip() or None
OUT_CSV = "airbnb_results.csv"

# Libellés et regex
LICENSE_LABELS = [
    "informations d’enregistrement","informations d'enregistrement",
    "numéro d’enregistrement","numéro d'enregistrement",
    "enregistrement","licence","license","registration","permit","dtcm","tourism"
]
HOST_SECTION_LABELS = [
    "Aller à la rencontre de votre hôte",
    "Faites connaissance avec votre hôte",
    "Rencontrez votre hôte",
    "Meet your host",
    "Get to know your host",
]
LICENSE_REGEX = re.compile(
    r"\b(?:DT\w{0,3}|DTCM|DED|RERA|BRN|Permit|Licen[cs]e|Reg(?:istration)?)?[-\s]?[A-Z0-9]{3,}[A-Z0-9/\- ]{0,20}\b",
    re.I,
)

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")

def human_sleep(sec: float) -> None:
    time.sleep(sec)

def click_show_more(page: Page, root) -> None:
    try:
        # Tente le bouton "Afficher plus" proche
        btn = root.get_by_role("button", name=re.compile(r"afficher plus|show more", re.I))
        if btn.count():
            btn.first.click(timeout=1500)
            human_sleep(0.2)
    except Exception:
        pass

def extract_license(page: Page) -> str:
    # Parcours des libellés connus
    for lbl in LICENSE_LABELS:
        nodes = page.get_by_text(lbl, exact=False)
        if not nodes.count():
            continue
        anch = nodes.first
        try:
            container = anch.locator("xpath=ancestor::*[self::section or self::div][1]")
            click_show_more(page, container)
            scope = container if container.count() else anch
            txt = _norm(scope.inner_text(timeout=2500))
        except Exception:
            continue
        m = LICENSE_REGEX.search(txt)
        if m:
            return _norm(m.group(0))
    # Secours: clique un éventuel "Afficher plus" global dans la description
    try:
        more = page.get_by_role("button", name=re.compile(r"afficher plus|show more", re.I))
        if more.count():
            more.first.click(timeout=1500)
            human_sleep(0.2)
    except Exception:
        pass
    # Scan global
    try:
        body = _norm(page.locator("body").inner_text(timeout=2500))
        m = LICENSE_REGEX.search(body)
        return _norm(m.group(0)) if m else ""
    except Exception:
        return ""

def extract_host_block(page: Page):
    # La carte hôte est en bas
    try:
        page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
    except Exception:
        pass
    human_sleep(0.4)
    for lbl in HOST_SECTION_LABELS:
        sec = page.get_by_text(lbl, exact=False)
        if sec.count():
            return sec.first.locator("xpath=ancestor::*[self::section or self::div][1]")
    cand = page.locator('[data-testid="pdp-host-profile-card"]')
    return cand.first if cand.count() else None

def extract_host_fields(page: Page):
    block = extract_host_block(page)
    name = rating = joined = profile_url = ""
    if not block:
        return name, rating, joined, profile_url

    # Lien profil
    prof = block.locator('a[href*="/users/show"]')
    if prof.count():
        try:
            href = prof.first.get_attribute("href") or ""
            if href:
                profile_url = href if href.startswith("http") else urljoin("https://www.airbnb.com", href)
        except Exception:
            pass

    # Texte bloc pour rating et joined
    body = ""
    try:
        body = _norm(block.inner_text(timeout=2500))
    except Exception:
        body = ""

    # Nom: exclut les étiquettes
    for sel in ['a[href*="/users/show"]','h2','h3','a[aria-label]','a','span','div']:
        els = block.locator(sel)
        limit = min(10, els.count())
        for i in range(limit):
            t = ""
            try:
                t = _norm(els.nth(i).inner_text(timeout=600))
            except Exception:
                t = ""
            if not t:
                continue
            low = t.lower()
            if any(k in low for k in ["hôte","host","rencontrez","faites connaissance","meet your host"]):
                continue
            if re.match(r"^[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÖØ-öø-ÿ' -]{1,59}$", t):
                name = t
                break
        if name:
            break

    # Note globale: "4,95 · 123 commentaires"
    m = re.search(r"\b([0-5](?:[.,]\d{1,2})?)\b\s*[·•]\s*\d+\s*(?:commentaires|avis|reviews)", body, re.I)
    if m:
        rating = m.group(1).replace(",", ".")

    # "Hôte depuis ..." ou "Joined in ..."
    for pat in [
        r"H[oô]te depuis\s+([A-Za-zéû]+\s+\d{4}|\d{4})",
        r"Joined in\s+([A-Za-z]+\s+\d{4}|\d{4})",
        r"Hosting since\s+([A-Za-z]+\s+\d{4}|\d{4})",
    ]:
        m = re.search(pat, body, re.I)
        if m:
            joined = _norm(m.group(1))
            break

    return name, rating, joined, profile_url

def extract_title(page: Page) -> str:
    try:
        return _norm(page.title())
    except Exception:
        return ""

def extract_price_text(page: Page) -> str:
    # Champ facultatif, on concatène les blocs prix si présents
    try:
        # Plusieurs variantes
        locs = [
            page.locator('[data-testid="book-it-default"]'),
            page.locator('[data-section-id="BOOK_IT_SIDEBAR"]'),
            page.get_by_text(re.compile(r"par nuit|per night|prix", re.I)),
        ]
        chunks = []
        for loc in locs:
            if loc.count():
                text = _norm(loc.first.inner_text(timeout=1200))
                if text:
                    chunks.append(text)
        return _norm(" | ".join(dict.fromkeys(chunks)))[:500]
    except Exception:
        return ""

def unique_room_links_from_search(page: Page, limit: int):
    # Récupère des liens /rooms/xxxxxxxx
    anchors = page.locator('a[href*="/rooms/"]')
    urls = set()
    count = anchors.count()
    for i in range(min(400, count)):
        try:
            href = anchors.nth(i).get_attribute("href") or ""
        except Exception:
            href = ""
        if not href:
            continue
        # Nettoyage
        u = href if href.startswith("http") else urljoin("https://www.airbnb.com", href)
        p = urlparse(u)
        if "/rooms/" in p.path and "adults" not in p.query:
            # Canonicalise sans fragments
            u = f"{p.scheme}://{p.netloc}{p.path}"
            urls.add(u)
        if len(urls) >= limit:
            break
    return list(urls)

def paginate_search_and_collect(page: Page, start_url: str, max_links: int, deadline: float):
    links = []
    page.goto(start_url, wait_until="domcontentloaded", timeout=45000)
    human_sleep(1.0)
    while len(links) < max_links and time.time() < deadline:
        # Scroll pour charger
        try:
            for _ in range(6):
                page.mouse.wheel(0, 2000)
                human_sleep(0.35)
        except Exception:
            pass
        batch = unique_room_links_from_search(page, max_links - len(links))
        # Ajoute en respectant l'ordre
        for u in batch:
            if u not in links:
                links.append(u)
                if len(links) >= max_links:
                    break
        # Essaie d'aller à la page suivante si besoin
        if len(links) < max_links:
            try:
                next_btn = page.get_by_role("button", name=re.compile(r"suivant|next", re.I))
                if next_btn.count():
                    next_btn.first.click(timeout=2500)
                    human_sleep(1.0)
                    continue
            except Exception:
                pass
            # Si pas de bouton, stop
            break
    return links

def scrape_room(page: Page, url: str):
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
    except PWTimeout:
        # Dernier recours full load
        page.goto(url, wait_until="load", timeout=60000)
    human_sleep(0.8)

    # Ferme éventuels overlays
    for sel in [
        'button[aria-label*="Fermer"]','button[aria-label*="Close"]',
        'button[aria-label*="dismiss"]','button[aria-label*="Ignorer"]'
    ]:
        try:
            btns = page.locator(sel)
            if btns.count():
                btns.first.click(timeout=800)
                human_sleep(0.2)
        except Exception:
            pass

    title = extract_title(page)
    license_code = extract_license(page)
    host_name, host_overall_rating, host_joined, host_profile_url = extract_host_fields(page)
    price_text = extract_price_text(page)

    return {
        "url": url,
        "title": title,
        "license_code": license_code,
        "host_name": host_name,
        "host_overall_rating": host_overall_rating,
        "host_profile_url": host_profile_url,
        "host_joined": host_joined,
        "price_text": price_text,
        "scraped_at": _now_iso(),
    }

def run():
    deadline = time.time() + MAX_MINUTES * 60

    launch_args = {
        "headless": True,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    }
    if PROXY:
        launch_args["proxy"] = {"server": PROXY}

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_args)
        ctx = browser.new_context(locale="fr-FR", user_agent=None)
        page = ctx.new_page()

        # Collecte des liens
        try:
            links = paginate_search_and_collect(page, START_URL, MAX_LISTINGS, deadline)
        except Exception:
            links = []
        # Si aucun lien, tente accès direct si on a collé une URL /rooms/ en START_URL
        if not links and "/rooms/" in START_URL:
            links = [START_URL]

        rows = []
        # Recyclage d'onglet pour limiter le bruit
        for url in links:
            if time.time() > deadline:
                break
            try:
                data = scrape_room(page, url)
            except Exception:
                data = {
                    "url": url, "title": "", "license_code": "", "host_name": "",
                    "host_overall_rating": "", "host_profile_url": "",
                    "host_joined": "", "price_text": "", "scraped_at": _now_iso(),
                }
            rows.append(data)

        ctx.close()
        browser.close()

    # Écriture CSV
    fieldnames = [
        "url","title","license_code","host_name","host_overall_rating",
        "host_profile_url","host_joined","price_text","scraped_at"
    ]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(130)
