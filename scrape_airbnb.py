# scrape_airbnb.py
# Playwright sync. Extrait: url, title, license_code, host_name, host_overall_rating,
# host_profile_url, host_joined, scraped_at.
import os, re, csv, time, datetime as dt, urllib.parse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL     = os.getenv("START_URL", "https://fr.airbnb.com/s/Dubai/homes")
MAX_LISTINGS  = int(os.getenv("MAX_LISTINGS", "10"))
MAX_MINUTES   = int(os.getenv("MAX_MINUTES", "10"))
PROXY         = os.getenv("PROXY") or None
OUT_CSV       = "airbnb_results.csv"

LICENSE_LABELS = re.compile(
    r"(enregistr|permit|licen[sc]e|dtcm|tourism|rera|case|escrow|et[0o]n|trade)",
    re.I
)
CODE_PATTERNS = [
    re.compile(r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}\b"),  # ex: BUR-BUR-OFXPS
    re.compile(r"\b\d{6,}\b"),                           # ex: 1100042
]

def absolutize(base_url: str, href: str) -> str:
    if not href:
        return ""
    href = href.split("?")[0]
    return urllib.parse.urljoin(base_url, href)

def collect_listing_urls(page, limit, max_minutes):
    start = time.time()
    page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
    seen, urls = set(), []
    while len(urls) < limit and (time.time()-start) < max_minutes*60:
        anchors = page.locator('a[href*="/rooms/"]:not([href*="experiences/"])')
        for i in range(anchors.count()):
            href = anchors.nth(i).get_attribute("href")
            href = absolutize(START_URL, href)
            if "/rooms/" not in href:
                continue
            rid = href.split("/rooms/")[1].split("/")[0]
            if not rid or not rid[0].isdigit():
                continue
            if href not in seen:
                seen.add(href)
                urls.append(href)
                print(f"#{len(urls)} {href}")
                if len(urls) >= limit:
                    break
        # scroll un peu pour charger plus
        page.mouse.wheel(0, 2000)
        page.wait_for_timeout(400)
        # pagination si visible
        try:
            nxt = page.locator('a[aria-label="Suivant"], a[aria-label="Next"]').first
            if nxt.is_visible():
                nxt.click()
                page.wait_for_load_state("domcontentloaded")
        except Exception:
            pass
    return urls[:limit]

def extract_license_from_text(text: str) -> str:
    if not text:
        return ""
    text_norm = " ".join(text.split())
    # fenêtre autour d’un libellé, puis regex du code
    for m in LICENSE_LABELS.finditer(text_norm):
        s = max(0, m.start()-160)
        e = min(len(text_norm), m.end()+160)
        window = text_norm[s:e]
        for pat in CODE_PATTERNS:
            m2 = pat.search(window)
            if m2:
                return m2.group(0)
    return ""

def find_host_section(page):
    # Restreint au bloc hôte, évite les avis
    candidates = [
        'section:has(h2:has-text("Faites connaissance avec votre hôte"))',
        'section:has(h2:has-text("Meet your Host"))',
        'section:has(h2:has-text("Get to know your host"))',
        'section:has(h2:has-text("Conoce a tu anfitri"))',
        'section:has(h2:has-text("Erfahre mehr über deinen Gastgeber"))',
    ]
    for sel in candidates:
        loc = page.locator(sel)
        if loc.count() and loc.first.is_visible():
            return loc.first
    return None

def extract_host_fields(page, listing_url):
    host_name = host_overall_rating = host_profile_url = host_joined = ""
    # scroll vers le bas pour charger le bloc hôte
    for _ in range(6):
        page.mouse.wheel(0, 1400)
        page.wait_for_timeout(250)
    sect = find_host_section(page)
    if not sect:
        # une dernière tentative après scroll total
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(700)
        except Exception:
            pass
        sect = find_host_section(page)
    if not sect:
        return host_name, host_overall_rating, host_profile_url, host_joined

    # URL du profil hôte (dans le bloc hôte uniquement)
    try:
        link = sect.locator('a[href^="/users/show/"]').first
        if link.count():
            href = link.get_attribute("href")
            host_profile_url = absolutize(listing_url, href)
    except Exception:
        pass

    # Nom de l’hôte
    try:
        # souvent le nom est le texte cliquable du même lien
        text = link.inner_text().strip() if link and link.count() else ""
        if not text:
            text = sect.locator('a[href^="/users/show/"]').first.inner_text().strip()
        if text and len(text) < 60:
            host_name = text
    except Exception:
        pass

    # Texte brut du bloc pour rating + année d’inscription
    try:
        block = sect.inner_text(timeout=3000)
    except Exception:
        block = ""

    # Note globale de l’hôte
    # formats vus: "4,63 ★", "4.9 ★", "4,9 • 49 évaluations"
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*[★*]", block)
    if not m:
        m = re.search(r"Note globale\s*:?[\s\n]*([0-9]+(?:[.,][0-9]+)?)", block, re.I)
    if not m:
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*[•·]\s*(?:avis|reviews)", block, re.I)
    if m:
        host_overall_rating = m.group(1).replace(",", ".")

    # Année/mois depuis quand sur Airbnb
    # formats vus: "sur Airbnb depuis 2016", "Membre depuis 2023", "Depuis juin 2019"
    m2 = re.search(r"(depuis|since)\s+(?:\w+\s+)?(\d{4})", block, re.I)
    if m2:
        host_joined = m2.group(2)

    return host_name, host_overall_rating, host_profile_url, host_joined

def extract_listing(page, url):
    title = license_code = ""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except PWTimeout:
        return {
            "url": url, "title": title, "license_code": license_code,
            "host_name": "", "host_overall_rating": "", "host_profile_url": "",
            "host_joined": "", "scraped_at": dt.datetime.utcnow().isoformat()
        }

    # titre
    try:
        title = page.locator('meta[property="og:title"]').first.get_attribute("content") or ""
        if not title:
            title = page.locator("h1").first.inner_text().strip()
    except Exception:
        title = ""

    # 1) CHEMIN INITIAL: scan du texte de toute la page
    try:
        body_text = page.locator("body").inner_text(timeout=8000)
    except Exception:
        body_text = ""
    license_code = extract_license_from_text(body_text)

    # 2) FALLBACK: ouvrir “À propos de ce logement” si rien trouvé
    if not license_code:
        try:
            # FR + EN libellés possibles
            btn = page.locator('button:has-text("Lire la suite"), button:has-text("Read more")').first
            if btn.count() and btn.is_visible():
                btn.click()
                modal = page.locator('[role="dialog"]').first
                modal_text = modal.inner_text(timeout=4000)
                license_code = extract_license_from_text(modal_text)
                page.keyboard.press("Escape")
        except Exception:
            pass

    # Champs hôte depuis le BLOC HÔTE (pas les commentaires)
    host_name, host_overall_rating, host_profile_url, host_joined = extract_host_fields(page, url)

    return {
        "url": url,
        "title": title,
        "license_code": license_code,
        "host_name": host_name,
        "host_overall_rating": host_overall_rating,
        "host_profile_url": host_profile_url,
        "host_joined": host_joined,
        "scraped_at": dt.datetime.utcnow().isoformat()
    }

def main():
    with sync_playwright() as p:
        launch_args = dict(headless=True, args=["--lang=fr-FR,fr"])
        if PROXY:
            launch_args["proxy"] = {"server": PROXY}
        browser = p.chromium.launch(**launch_args)
        context = browser.new_context(locale="fr-FR")
        page = context.new_page()

        print("START", START_URL)
        try:
            urls = collect_listing_urls(page, MAX_LISTINGS, MAX_MINUTES)
        except PWTimeout:
            urls = []
        print(f"FOUND_URLS {len(urls)}")

        rows = []
        for url in urls:
            try:
                rows.append(extract_listing(page, url))
            except Exception as e:
                rows.append({
                    "url": url, "title": "", "license_code": "",
                    "host_name": "", "host_overall_rating": "",
                    "host_profile_url": "", "host_joined": "",
                    "scraped_at": dt.datetime.utcnow().isoformat()
                })

        # écriture CSV (UTF-8 BOM pour Excel)
        fieldnames = ["url","title","license_code","host_name","host_overall_rating",
                      "host_profile_url","host_joined","scraped_at"]
        with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"SAVED {len(rows)} rows to {OUT_CSV}")

        context.close()
        browser.close()

if __name__ == "__main__":
    main()
