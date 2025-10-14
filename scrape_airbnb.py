# scrape_airbnb.py
# Champs CSV: url,title,license_code,host_name,host_overall_rating,host_profile_url,host_joined,price_text,scraped_at

import os, re, sys, csv, time
from datetime import datetime
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "50"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", "10"))
PROXY = os.getenv("PROXY", "").strip() or None
OUT_CSV = "airbnb_results.csv"

LICENSE_LABELS = [
    "informations d’enregistrement","informations d'enregistrement",
    "numéro d’enregistrement","numéro d'enregistrement",
    "enregistrement","licence","license","registration","permit","dtcm","tourism",
    "registration number","licence number","tourism licence","permit number"
]
LICENSE_REGEX = re.compile(
    r"\b(?:DT\w{0,3}|DTCM|DED|RERA|BRN|Permit|Licen[cs]e|Reg(?:istration)?)?[-\s]?[A-Z0-9]{3,}[A-Z0-9/\- ]{0,20}\b",
    re.I,
)

HOST_SECTION_LABELS = [
    "Aller à la rencontre de votre hôte","Faites connaissance avec votre hôte",
    "Rencontrez votre hôte","Meet your host","Get to know your host"
]

def _norm(s): return re.sub(r"\s+", " ", s or "").strip()
def _now(): return datetime.utcnow().isoformat(timespec="seconds")
def nap(s): time.sleep(s)

def accept_cookies_and_close_popups(page):
    # Cookies
    for txt in [
        r"Tout accepter",r"Accepter",r"Accept all",r"Accept & continue",r"OK",
        r"J'accepte",r"Autoriser tous",r"Allow all",
    ]:
        try:
            btn = page.get_by_role("button", name=re.compile(txt, re.I))
            if btn.count():
                btn.first.click(timeout=1200); nap(0.2); break
        except Exception: pass
    # Modaux
    for sel in [
        'button[aria-label*="Fermer"]','button[aria-label*="Close"]',
        'button[aria-label*="Dismiss"]','[data-testid="modal-close"] button'
    ]:
        try:
            b = page.locator(sel)
            if b.count():
                b.first.click(timeout=800); nap(0.2)
        except Exception: pass

def click_show_more_near(locator):
    try:
        btn = locator.get_by_role("button", name=re.compile(r"afficher plus|show more", re.I))
        if btn.count():
            btn.first.click(timeout=1500); nap(0.2)
    except Exception: pass

def extract_license(page):
    # 1) blocs politiques / infos
    for lbl in LICENSE_LABELS:
        nodes = page.get_by_text(lbl, exact=False)
        if not nodes.count(): continue
        anchor = nodes.first
        try:
            container = anchor.locator("xpath=ancestor::*[self::section or self::div][1]")
            click_show_more_near(container)
            txt = _norm((container if container.count() else anchor).inner_text(timeout=2500))
        except Exception: txt = ""
        if not txt: continue
        m = LICENSE_REGEX.search(txt)
        if m: return _norm(m.group(0))
    # 2) bouton "Afficher plus" global de la description
    try:
        btn = page.get_by_role("button", name=re.compile(r"afficher plus|show more", re.I))
        if btn.count(): btn.first.click(timeout=1500); nap(0.3)
    except Exception: pass
    # 3) scan global en secours
    try:
        body = _norm(page.locator("body").inner_text(timeout=3000))
        m = LICENSE_REGEX.search(body)
        return _norm(m.group(0)) if m else ""
    except Exception:
        return ""

def host_block(page):
    try: page.evaluate("()=>window.scrollTo(0,document.body.scrollHeight)")
    except Exception: pass
    nap(0.5)
    for lbl in HOST_SECTION_LABELS:
        node = page.get_by_text(lbl, exact=False)
        if node.count():
            return node.first.locator("xpath=ancestor::*[self::section or self::div][1]")
    for sel in ['[data-testid="pdp-host-profile-card"]','[data-testid="pdp-host-info-card"]']:
        b = page.locator(sel)
        if b.count(): return b.first
    return None

def extract_host(page):
    blk = host_block(page)
    name = rating = joined = profurl = ""
    if not blk: return name, rating, joined, profurl

    # lien profil
    a = blk.locator('a[href*="/users/show"]')
    if a.count():
        try:
            href = a.first.get_attribute("href") or ""
            if href: profurl = href if href.startswith("http") else urljoin("https://www.airbnb.com", href)
        except Exception: pass

    text = ""
    try: text = _norm(blk.inner_text(timeout=3000))
    except Exception: text = ""

    # nom: cherche un texte court non-label
    candidates = []
    for sel in ['a[href*="/users/show"]','h2','h3','[data-testid="user-profile-name"]','a[aria-label]','a','span','div']:
        els = blk.locator(sel)
        for i in range(min(10, els.count())):
            t = ""
            try: t = _norm(els.nth(i).inner_text(timeout=600))
            except Exception: t = ""
            if not t: continue
            low = t.lower()
            if any(k in low for k in ["hôte","host","rencontrez","faites connaissance","meet your host"]): continue
            candidates.append(t)
    for t in candidates:
        if re.match(r"^[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÖØ-öø-ÿ' -]{1,59}$", t):
            name = t; break
    if not name and candidates: name = candidates[0][:60]

    # note globale
    m = re.search(r"\b([0-5](?:[.,]\d{1,2})?)\b\s*[·•]\s*\d+\s*(?:commentaires|avis|reviews)", text, re.I)
    if m: rating = m.group(1).replace(",", ".")

    # hôte depuis
    for pat in [r"H[oô]te depuis\s+([A-Za-zéû]+\s+\d{4}|\d{4})",
                r"Joined in\s+([A-Za-z]+\s+\d{4}|\d{4})",
                r"Hosting since\s+([A-Za-z]+\s+\d{4}|\d{4})"]:
        m = re.search(pat, text, re.I)
        if m: joined = _norm(m.group(1)); break

    return name, rating, joined, profurl

def extract_title(page):
    try: return _norm(page.title())
    except Exception: return ""

def extract_price(page):
    try:
        locs = [
            page.locator('[data-testid="book-it-default"]'),
            page.locator('[data-section-id="BOOK_IT_SIDEBAR"]'),
            page.get_by_text(re.compile(r"par nuit|per night|prix", re.I)),
        ]
        chunks = []
        for loc in locs:
            if loc.count():
                t = _norm(loc.first.inner_text(timeout=1200))
                if t: chunks.append(t)
        return _norm(" | ".join(dict.fromkeys(chunks)))[:500]
    except Exception:
        return ""

def collect_room_links(page, limit):
    seen = set(); out = []
    anchors = page.locator('a[href*="/rooms/"]')
    n = min(800, anchors.count())
    for i in range(n):
        try: href = anchors.nth(i).get_attribute("href") or ""
        except Exception: href = ""
        if not href: continue
        u = href if href.startswith("http") else urljoin("https://www.airbnb.com", href)
        p = urlparse(u)
        if "/rooms/" in p.path:
            u = f"{p.scheme}://{p.netloc}{p.path}"
            if u not in seen:
                seen.add(u); out.append(u)
                if len(out) >= limit: break
    return out

def paginate_and_collect(page, start_url, max_links, deadline_ts):
    page.goto(start_url, wait_until="domcontentloaded", timeout=45000)
    accept_cookies_and_close_popups(page)
    nap(0.8)
    links = []
    while len(links) < max_links and time.time() < deadline_ts:
        # scroll fort
        for _ in range(10):
            try: page.mouse.wheel(0, 2200)
            except Exception: pass
            nap(0.25)
        batch = collect_room_links(page, max_links - len(links))
        for u in batch:
            if u not in links:
                links.append(u)
                if len(links) >= max_links: break
        if len(links) >= max_links: break
        # bouton suivant
        try:
            nxt = page.get_by_role("button", name=re.compile(r"suivant|next", re.I))
            if nxt.count():
                nxt.first.click(timeout=2500); nap(1.0); continue
        except Exception: pass
        break
    return links

def open_room(page, url):
    try: page.goto(url, wait_until="domcontentloaded", timeout=45000)
    except PWTimeout: page.goto(url, wait_until="load", timeout=60000)
    nap(0.8)
    accept_cookies_and_close_popups(page)

def scrape_room(page, url):
    open_room(page, url)
    title = extract_title(page)
    license_code = extract_license(page)
    host_name, host_overall_rating, host_joined, host_profile_url = extract_host(page)
    price_text = extract_price(page)
    return {
        "url": url, "title": title, "license_code": license_code,
        "host_name": host_name, "host_overall_rating": host_overall_rating,
        "host_profile_url": host_profile_url, "host_joined": host_joined,
        "price_text": price_text, "scraped_at": _now(),
    }

def run():
    deadline = time.time() + MAX_MINUTES * 60
    launch_args = {
        "headless": True,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox","--disable-dev-shm-usage",
        ],
    }
    if PROXY: launch_args["proxy"] = {"server": PROXY}

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_args)
        ctx = browser.new_context(
            locale="fr-FR",
            viewport={"width": 1366, "height": 768},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"),
        )
        # webdriver=false
        ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined});")
        page = ctx.new_page()

        try: links = paginate_and_collect(page, START_URL, MAX_LISTINGS, deadline)
        except Exception: links = []
        if not links and "/rooms/" in START_URL: links = [START_URL]

        rows = []
        for url in links:
            if time.time() > deadline: break
            try: rows.append(scrape_room(page, url))
            except Exception:
                rows.append({"url": url, "title": "", "license_code": "", "host_name": "",
                             "host_overall_rating": "", "host_profile_url": "",
                             "host_joined": "", "price_text": "", "scraped_at": _now()})
        ctx.close(); browser.close()

    fields = ["url","title","license_code","host_name","host_overall_rating",
              "host_profile_url","host_joined","price_text","scraped_at"]
    with open(OUT_CSV,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader()
        for r in rows: w.writerow(r)

if __name__ == "__main__":
    try: run()
    except KeyboardInterrupt: sys.exit(130)
