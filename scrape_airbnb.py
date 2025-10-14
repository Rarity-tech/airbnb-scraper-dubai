# scrape_airbnb.py
import os, csv, re, time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ------------------------- Config via ENV -------------------------
START_URL = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST = int(os.getenv("MAX_LISTINGS", "30"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", "10"))
PROXY = os.getenv("PROXY", "").strip() or None

OUT_CSV = "airbnb_results.csv"
FIELDS = [
    "url",
    "title",
    "license_code",
    "host_name",
    "host_overall_rating",
    "host_profile_url",
    "host_joined",
    "scraped_at",
]

# ------------------------- Helpers -------------------------
def clean(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def accept_cookies(page):
    # French and English variants
    selectors = [
        "button:has-text('Tout accepter')",
        "button:has-text('Accepter tout')",
        "button:has-text('Accepter')",
        "button:has-text('OK')",
        "button:has-text('I agree')",
        "button:has-text('Accept all')",
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if btn.first.is_visible():
                btn.first.click(timeout=1000)
                break
        except Exception:
            pass

def wait_dom(page, timeout=20000):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=timeout)
    except PWTimeout:
        pass

def absolute_room_url(cur_url, href):
    if not href:
        return None
    href = href.split("?")[0]
    absu = urljoin(cur_url, href)
    p = urlparse(absu)
    if "/rooms/" not in p.path:
        return None
    return f"{p.scheme}://{p.netloc}{p.path}"

# ------------------------- URL collection -------------------------
def collect_listing_urls(page, max_list, max_minutes):
    page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
    accept_cookies(page)
    seen = set()
    start = time.time()
    last_added = 0
    while len(seen) < max_list and (time.time() - start) < max_minutes * 60:
        try:
            page.wait_for_selector("a[href*='/rooms/']", timeout=8000)
        except PWTimeout:
            pass

        anchors = page.locator("a[href*='/rooms/']").all()
        for a in anchors:
            try:
                href = a.get_attribute("href")
            except Exception:
                href = None
            u = absolute_room_url(page.url, href)
            if u and u not in seen:
                seen.add(u)
                last_added = time.time()

        # If nothing new depuis 6s, essaye un clic « Afficher plus »
        if time.time() - last_added > 6:
            try:
                page.get_by_role("button", name=re.compile("Afficher plus|Show more", re.I)).first.click(timeout=1000)
            except Exception:
                pass

        # Scroll
        try:
            page.mouse.wheel(0, 2500)
            time.sleep(0.7)
        except Exception:
            break

    urls = list(seen)[:max_list]
    urls.sort()
    print(f"FOUND_URLS {len(urls)}")
    for i, u in enumerate(urls, 1):
        print(f"#{i} {u}")
    return urls

# ------------------------- License extraction -------------------------
_LICENSE_PATTERNS = [
    re.compile(r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}\b"),  # BUR-BUR-OFXPS
    re.compile(r"\b\d{6,}\b"),                           # 1100042 etc.
]
_LICENSE_LABELS = re.compile(
    r"(Infos? d['’]enregistrement|D[ée]tails de l['’]enregistrement|Registration|Permit|License|Licence|Tourism|DTCM)",
    re.I,
)

def open_about_and_get_text(page) -> str:
    # Section « À propos de ce logement » / « About this place »
    sec = page.locator(":is(section,div)").filter(
        has_text=re.compile(r"À propos de ce logement|About this place", re.I)
    ).first
    if not sec or not sec.count():
        return ""

    # Bouton Lire la suite / Afficher plus / Read more
    try:
        btn = sec.get_by_role(
            "button", name=re.compile(r"(Lire la suite|Afficher plus|Read more|Show more)", re.I)
        ).first
        if btn.is_visible():
            btn.click(timeout=3000)
            # Dialog
            dlg = page.get_by_role("dialog").first
            dlg.wait_for(state="visible", timeout=5000)
            txt = clean(dlg.inner_text(timeout=5000))
            # Fermer
            try:
                page.keyboard.press("Escape")
            except Exception:
                try:
                    dlg.get_by_role("button", name=re.compile("Fermer|Close|×", re.I)).first.click(timeout=1000)
                except Exception:
                    pass
            return txt
    except Exception:
        pass

    # Fallback: texte de la section
    try:
        return clean(sec.inner_text(timeout=3000))
    except Exception:
        return ""

def extract_license_code(page) -> str:
    # D’abord, tente via le modal
    about_txt = open_about_and_get_text(page)
    text_sources = [about_txt]

    # Fallback: tout le body
    try:
        text_sources.append(clean(page.locator("body").inner_text(timeout=3000)))
    except Exception:
        pass

    for txt in text_sources:
        if not txt:
            continue
        # Priorité: près des labels
        if _LICENSE_LABELS.search(txt):
            # Cherche le premier pattern valide après label
            for pat in _LICENSE_PATTERNS:
                m = pat.search(txt)
                if m:
                    return m.group(0)
        # Sinon, premier pattern global
        for pat in _LICENSE_PATTERNS:
            m = pat.search(txt)
            if m:
                return m.group(0)
    return ""

# ------------------------- Host extraction -------------------------
def get_host_section(page):
    return page.locator(":is(section,div,main)").filter(
        has_text=re.compile(r"Faites connaissance avec votre h[ôo]te|Meet your host", re.I)
    ).first

def extract_host_core(page):
    """
    Retourne (host_name, host_profile_url, section_text)
    - URL: uniquement le lien « Voir le profil / View profile » dans la carte hôte
    - Exclut toute zone d’avis/commentaire
    """
    sec = get_host_section(page)
    if not sec or not sec.count():
        return "", "", ""

    profile_url = ""
    # 1) Lien explicite « Voir le profil »
    try:
        a_profile = sec.get_by_role("link", name=re.compile(r"Voir le profil|View profile", re.I)).first
        href = a_profile.get_attribute("href", timeout=2000)
        if href:
            profile_url = urljoin(page.url, href)
    except Exception:
        pass

    # 2) Fallback: 1er /users/show/ dans la carte hôte qui n’est pas un avis
    if not profile_url:
        try:
            links = sec.locator("a[href^='/users/show/']").all()
            for lk in links:
                try:
                    ancestor_txt = clean(lk.locator("xpath=ancestor-or-self::*[1]").inner_text(timeout=1500))
                except Exception:
                    ancestor_txt = ""
                if re.search(r"\b(avis|commentaire|review)\b", ancestor_txt, re.I):
                    continue
                href = lk.get_attribute("href")
                if href:
                    profile_url = urljoin(page.url, href)
                    break
        except Exception:
            pass

    # 3) Nom de l’hôte
    host_name = ""
    # alt d’avatar souvent = nom
    try:
        img = sec.locator("img[alt]").first
        alt = clean(img.get_attribute("alt", timeout=1500) or "")
        if alt and not re.search(r"\b(airbnb|profil|profile)\b", alt, re.I):
            host_name = alt
    except Exception:
        pass
    # Fallback titres/texte voisin
    if not host_name:
        for sel in ["[data-testid*='host-profile'] h2", "[data-testid*='host-profile'] h3", "h2", "h3"]:
            try:
                t = clean(sec.locator(sel).first.inner_text(timeout=1500))
                m = re.search(r"^([A-ZÀ-ÖØ-Þ][\wÀ-ÖØ-öø-ÿ'’.-]{1,40})\b", t)
                if m:
                    host_name = m.group(1)
                    break
            except Exception:
                pass

    # 4) Texte complet de la carte hôte
    try:
        section_text = clean(sec.inner_text(timeout=2500))
    except Exception:
        section_text = ""

    return host_name, profile_url, section_text

def extract_host_rating(section_text: str) -> str:
    if not section_text:
        return ""
    # Ex: "4,63★", "4.9 ★", "4,8 sur 5"
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:sur\s*5)?\s*[★⭐]", section_text, re.I)
    return m.group(1).replace(",", ".") if m else ""

def extract_host_joined(section_text: str) -> str:
    if not section_text:
        return ""
    # "depuis 2019" / "since 2019"
    m = re.search(r"(?:depuis|since)\s+(\d{4})", section_text, re.I)
    if m:
        return m.group(1)
    # "2 ans sur Airbnb", "8 mois sur Airbnb"
    m = re.search(r"\b(\d+)\s*(ans?|mois)\s+sur\s+Airbnb\b", section_text, re.I)
    if m:
        return f"{m.group(1)} {m.group(2)}".replace("  ", " ")
    # "Member since 2019"
    m = re.search(r"Member\s+since\s+(\d{4})", section_text, re.I)
    if m:
        return m.group(1)
    # Dernier recours: 4 chiffres plausibles dans la carte
    m = re.search(r"\b(20\d{2}|19\d{2})\b", section_text)
    return m.group(1) if m else ""

# ------------------------- Listing parsing -------------------------
def parse_listing(page, url: str) -> dict:
    row = {k: "" for k in FIELDS}
    row["url"] = url
    row["scraped_at"] = now_iso()

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except PWTimeout:
        # second try with networkidle relaxed
        try:
            page.goto(url, wait_until="load", timeout=60000)
        except Exception:
            return row

    accept_cookies(page)
    wait_dom(page)

    # Title
    try:
        title = clean(page.locator("h1:visible").first.inner_text(timeout=4000))
    except Exception:
        title = ""
    row["title"] = title

    # License
    try:
        row["license_code"] = extract_license_code(page)
    except Exception:
        row["license_code"] = ""

    # Host block
    host_name, host_profile_url, host_block_txt = extract_host_core(page)
    row["host_name"] = host_name
    row["host_profile_url"] = host_profile_url
    row["host_overall_rating"] = extract_host_rating(host_block_txt)
    row["host_joined"] = extract_host_joined(host_block_txt)

    return row

# ------------------------- Main -------------------------
def main():
    t0 = time.time()
    with sync_playwright() as pw:
        launch_kwargs = {
            "headless": True,
            "args": ["--disable-dev-shm-usage", "--no-sandbox"],
        }
        if PROXY:
            launch_kwargs["proxy"] = {"server": PROXY}
        browser = pw.chromium.launch(**launch_kwargs)

        context = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/118 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"},
            viewport={"width": 1366, "height": 900},
        )
        page = context.new_page()

        urls = collect_listing_urls(page, MAX_LIST, MAX_MINUTES)

        rows = []
        for u in urls:
            try:
                r = parse_listing(page, u)
            except Exception:
                r = {k: "" for k in FIELDS}
                r["url"] = u
                r["scraped_at"] = now_iso()
            rows.append(r)

        context.close()
        browser.close()

    # Write CSV
    with open(OUT_CSV, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    print(f"SAVED {len(rows)} rows to {OUT_CSV}")
    print(f"DURATION {round(time.time()-t0,1)}s")

if __name__ == "__main__":
    main()
