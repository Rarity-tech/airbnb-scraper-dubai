# scrape_airbnb.py
import os, csv, re, time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------------- Config ----------------
START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.getenv("MAX_LISTINGS", "30"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", "10"))
PROXY       = os.getenv("PROXY", "").strip() or None

OUT_CSV = "airbnb_results.csv"
FIELDS = [
    "url","title","license_code","host_name",
    "host_overall_rating","host_profile_url","host_joined","scraped_at"
]

# ---------------- Utils ----------------
def clean(s:str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def accept_cookies(page):
    for sel in [
        "button:has-text('Tout accepter')",
        "button:has-text('Accepter tout')",
        "button:has-text('Accepter')",
        "button:has-text('I agree')",
        "button:has-text('Accept all')",
        "button[aria-label*='accepter' i]",
    ]:
        try:
            b = page.locator(sel)
            if b.first.is_visible():
                b.first.click(timeout=1000)
                break
        except Exception:
            pass

def wait_dom(page, timeout=20000):
    try: page.wait_for_load_state("domcontentloaded", timeout=timeout)
    except PWTimeout: pass

def absolute_room_url(cur_url, href):
    if not href: return None
    href = href.split("?")[0]
    u = urljoin(cur_url, href)
    p = urlparse(u)
    if "/rooms/" not in p.path: return None
    return f"{p.scheme}://{p.netloc}{p.path}"

# ------------- Collect listing URLs -------------
def collect_listing_urls(page, max_list, max_minutes):
    page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
    accept_cookies(page)
    seen=set(); start=time.time(); last=time.time()

    while len(seen)<max_list and (time.time()-start)<max_minutes*60:
        try: page.wait_for_selector("a[href*='/rooms/']", timeout=8000)
        except PWTimeout: pass

        for a in page.locator("a[href*='/rooms/']").all():
            try: href=a.get_attribute("href")
            except Exception: href=None
            u = absolute_room_url(page.url, href)
            if u and u not in seen:
                seen.add(u); last=time.time()

        # scroll/feed
        page.mouse.wheel(0, 2500); time.sleep(0.6)
        if time.time()-last>5:
            try:
                page.get_by_role("button", name=re.compile("Afficher plus|Show more", re.I)).first.click(timeout=800)
            except Exception: pass

    urls=sorted(list(seen))[:max_list]
    print(f"FOUND_URLS {len(urls)}"); [print(f"#{i+1} {u}") for i,u in enumerate(urls)]
    return urls

# ------------- License extraction -------------
_LICENSE_PATTERNS = [
    re.compile(r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}\b"),
    re.compile(r"\b\d{6,}\b"),
]
_LICENSE_LABELS = re.compile(
    r"(Infos? d['’]enregistrement|D[ée]tails de l['’]enregistrement|Registration|Permit|License|Licence|Tourism|DTCM)",
    re.I,
)

def open_about_and_get_text(page) -> str:
    # clique le bouton global "Lire la suite / Read more" qui ouvre le dialog
    try:
        btn = page.get_by_role("button",
                name=re.compile(r"(Lire la suite|Afficher plus|Read more|Show more)", re.I)
              ).first
        if btn.is_visible():
            btn.click(timeout=3000)
            dlg = page.get_by_role("dialog").first
            dlg.wait_for(state="visible", timeout=5000)
            txt = clean(dlg.inner_text(timeout=5000))
            try: page.keyboard.press("Escape")
            except Exception:
                try: dlg.get_by_role("button", name=re.compile("Fermer|Close|×", re.I)).first.click(timeout=1000)
                except Exception: pass
            return txt
    except Exception:
        pass

    # fallback: section si visible sans modal
    try:
        sec = page.locator(":is(section,div)").filter(
            has_text=re.compile(r"À propos de ce logement|About this place", re.I)
        ).first
        if sec.count():
            return clean(sec.inner_text(timeout=3000))
    except Exception:
        pass

    # dernier recours: body
    try: return clean(page.locator("body").inner_text(timeout=3000))
    except Exception: return ""

def extract_license_code(page) -> str:
    txt = open_about_and_get_text(page)
    for scope in (txt, ):
        if not scope: continue
        if _LICENSE_LABELS.search(scope):
            for pat in _LICENSE_PATTERNS:
                m = pat.search(scope)
                if m: return m.group(0)
        for pat in _LICENSE_PATTERNS:
            m = pat.search(scope)
            if m: return m.group(0)
    return ""

# ------------- Host extraction -------------
HOST_HEADING_RX = re.compile(
    r"(Faites connaissance avec votre h[ôo]te|Meet your host|Get to know your host)",
    re.I,
)

def ensure_host_section_loaded(page, max_scrolls=30):
    """Scroll jusqu’à ce que la carte hôte existe dans le DOM."""
    for _ in range(max_scrolls):
        if page.locator(":is(section,div,main)").filter(has_text=HOST_HEADING_RX).first.count():
            return True
        page.mouse.wheel(0, 1800)
        time.sleep(0.35)
    return page.locator(":is(section,div,main)").filter(has_text=HOST_HEADING_RX).first.count()>0

def get_host_section(page):
    return page.locator(":is(section,div,main)").filter(has_text=HOST_HEADING_RX).first

def extract_host_core(page):
    """Retourne (name, profile_url, section_text) à partir **exclusivement** de la carte hôte."""
    if not ensure_host_section_loaded(page):
        return "", "", ""
    sec = get_host_section(page)
    if not sec or not sec.count():
        return "", "", ""

    # Nom: alt d’avatar puis titres de la carte
    name = ""
    try:
        alt = sec.locator("img[alt]").first.get_attribute("alt", timeout=1500)
        alt = clean(alt or "")
        if alt and not re.search(r"\b(airbnb|profil|profile)\b", alt, re.I):
            name = alt
    except Exception: pass
    if not name:
        for sel in ["h2","h3","[data-testid*='host-profile'] h2","[data-testid*='host-profile'] h3"]:
            try:
                t = clean(sec.locator(sel).first.inner_text(timeout=1500))
                m = re.search(r"^([A-ZÀ-ÖØ-Þ][\wÀ-ÖØ-öø-ÿ'’.-]{1,40})\b", t)
                if m: name=m.group(1); break
            except Exception: pass

    # URL profil: liens /users/show/ existant dans la **carte hôte** (pas les avis)
    profile = ""
    try:
        # priorité à un lien autour du nom ou de l’avatar
        if name:
            for lk in sec.locator("a[href^='/users/show/']").all():
                try:
                    t = clean(lk.inner_text(timeout=800))
                except Exception:
                    t = ""
                if not t or name.lower() in t.lower():
                    href = lk.get_attribute("href")
                    if href:
                        profile = urljoin(page.url, href); break
        if not profile:
            lk = sec.locator("a[href^='/users/show/']").first
            if lk.count():
                href = lk.get_attribute("href")
                if href: profile = urljoin(page.url, href)
    except Exception: pass

    # Texte brut de la carte
    try: sec_txt = clean(sec.inner_text(timeout=2500))
    except Exception: sec_txt = ""

    return name, profile, sec_txt

def extract_host_rating(sec_txt:str) -> str:
    if not sec_txt: return ""
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:sur\s*5)?\s*[★⭐]", sec_txt, re.I)
    return m.group(1).replace(",", ".") if m else ""

def extract_host_joined(sec_txt:str) -> str:
    if not sec_txt: return ""
    m = re.search(r"(?:depuis|since)\s+(\d{4})", sec_txt, re.I)
    if m: return m.group(1)
    m = re.search(r"\b(\d+)\s*(ans?|mois)\s+sur\s+Airbnb\b", sec_txt, re.I)
    if m: return f"{m.group(1)} {m.group(2)}"
    m = re.search(r"Member\s+since\s+(\d{4})", sec_txt, re.I)
    if m: return m.group(1)
    m = re.search(r"\b(20\d{2}|19\d{2})\b", sec_txt)
    return m.group(1) if m else ""

# ------------- Parse one listing -------------
def parse_listing(page, url:str) -> dict:
    row = {k:"" for k in FIELDS}; row["url"]=url; row["scraped_at"]=now_iso()

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
    except PWTimeout:
        try: page.goto(url, wait_until="load", timeout=60000)
        except Exception: return row

    accept_cookies(page); wait_dom(page)

    # Titre
    try: row["title"] = clean(page.locator("h1:visible").first.inner_text(timeout=5000))
    except Exception: pass

    # Licence
    try: row["license_code"] = extract_license_code(page)
    except Exception: pass

    # Hôte
    try:
        host_name, host_profile, sec_txt = extract_host_core(page)
        row["host_name"] = host_name
        row["host_profile_url"] = host_profile
        row["host_overall_rating"] = extract_host_rating(sec_txt)
        row["host_joined"] = extract_host_joined(sec_txt)
    except Exception: pass

    return row

# ------------- Main -------------
def main():
    t0 = time.time()
    with sync_playwright() as pw:
        launch = {"headless": True, "args": ["--disable-dev-shm-usage","--no-sandbox"]}
        if PROXY: launch["proxy"] = {"server": PROXY}
        browser = pw.chromium.launch(**launch)
        context = browser.new_context(
            locale="fr-FR",
            viewport={"width":1366,"height":900},
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/118 Safari/537.36"),
            extra_http_headers={"Accept-Language":"fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        page = context.new_page()

        urls = collect_listing_urls(page, MAX_LIST, MAX_MINUTES)

        rows=[]
        for u in urls:
            try: rows.append(parse_listing(page, u))
            except Exception:
                r = {k:"" for k in FIELDS}; r["url"]=u; r["scraped_at"]=now_iso(); rows.append(r)

        context.close(); browser.close()

    with open(OUT_CSV,"w",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f, fieldnames=FIELDS); w.writeheader(); w.writerows(rows)

    print(f"SAVED {len(rows)} rows to {OUT_CSV}")
    print(f"DURATION {round(time.time()-t0,1)}s")

if __name__ == "__main__":
    main()
