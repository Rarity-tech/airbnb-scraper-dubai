#!/usr/bin/env python3
import os, csv, time, random, json, re
from datetime import datetime, timedelta
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# --------- Entrées (via workflow) ----------
START_URL   = os.environ.get("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.environ.get("MAX_LISTINGS", "10"))
MAX_MINUTES = int(os.environ.get("MAX_MINUTES", "10"))
PROXY       = os.environ.get("PROXY", "").strip()

# Dates facultatives pour forcer l’affichage d’un prix
CHECK_IN    = os.environ.get("CHECK_IN", "").strip()    # ex: 2025-11-10
CHECK_OUT   = os.environ.get("CHECK_OUT", "").strip()   # ex: 2025-11-12
ADULTS      = os.environ.get("ADULTS", "1").strip()

OUTPUT_CSV  = "airbnb_results.csv"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

# --------- Utils ----------
LICENSE_LABELS = [
    "informations d’enregistrement", "informations d'enregistrement",
    "numéro d’enregistrement", "numéro d'enregistrement",
    "registration number", "registration", "licence", "license",
    "permit", "dtcm", "tourism"
]

HOST_SECTION_LABELS = [
    "aller à la rencontre de votre hôte", "faites connaissance avec votre hôte",
    "meet your host", "get to know your host", "hosted by", "l’hôte", "l'hôte"
]

def norm(txt: str) -> str:
    if not txt: return ""
    txt = txt.replace("\u00a0", " ")
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def build_listing_url(base: str) -> str:
    if not (CHECK_IN and CHECK_OUT):
        return base
    u = urlparse(base)
    q = parse_qs(u.query)
    q.update({"check_in":[CHECK_IN], "check_out":[CHECK_OUT], "adults":[ADULTS]})
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q, doseq=True), u.fragment))

def click_all_show_more(page):
    # Essaie d’ouvrir tout ce qui ressemble à “Afficher plus / Show more”
    candidates = [
        "Afficher plus", "Afficher tout", "Voir plus", "Plus",
        "Show more", "See more", "Read more", "More"
    ]
    for label in candidates:
        try:
            # clique plusieurs fois si présent à divers endroits
            for _ in range(3):
                btn = page.get_by_text(label, exact=False)
                if btn.count():
                    btn.first.click(timeout=1000)
                    time.sleep(0.3)
        except Exception:
            continue

def page_text(page) -> str:
    try:
        return page.locator("body").inner_text(timeout=1500)
    except Exception:
        return ""

def find_license_in_text(txt: str) -> str:
    if not txt: return ""
    low = txt.lower()

    if not any(lbl in low for lbl in LICENSE_LABELS):
        return ""

    # Heuristiques de capture
    patterns = [
        r"(?:registration|licen[cs]e|permit|dtcm)[^:\n]*:\s*([A-Za-z0-9\-_/\. ]{4,40})",
        r"(?:num[eé]ro d[’']enregistrement)[^:\n]*:\s*([A-Za-z0-9\-_/\. ]{4,40})",
        r"(dtcm)\s*[:\-]?\s*([A-Za-z0-9\-_/\. ]{4,40})",
        r"(?:tourism|department\s+of\s+tourism)[^:\n]*:\s*([A-Za-z0-9\-_/\. ]{4,40})"
    ]
    for pat in patterns:
        m = re.search(pat, low, flags=re.IGNORECASE)
        if m:
            # Prend le dernier groupe non vide
            groups = [g for g in m.groups() if g and not g.lower().startswith(("registration","license","permit","numéro","numéro d","dtcm","tourism"))]
            if groups:
                return norm(groups[-1]).upper()

    # Fallback: ligne après mot-clé
    for lbl in LICENSE_LABELS:
        if lbl in low:
            idx = low.find(lbl)
            snippet = txt[idx: idx+200]
            lines = [norm(s) for s in snippet.split("\n") if s.strip()]
            if len(lines) >= 2:
                cand = lines[1]
                # nettoie préfixes
                cand = re.sub(r"^(?:[:\-–]|\s)*", "", cand)
                return norm(cand).upper()
    return ""

def collect_listing_urls(page, target_count, deadline):
    urls = set()
    tries = 0
    while len(urls) < target_count and datetime.utcnow() < deadline and tries < 12:
        anchors = page.locator("a[href*='/rooms/']")
        n = anchors.count()
        for i in range(n):
            href = anchors.nth(i).get_attribute("href")
            if not href or "/rooms/" not in href:
                continue
            if href.startswith("/"):
                href = "https://www.airbnb.com" + href
            href = href.split("?")[0]
            urls.add(href)
            if len(urls) >= target_count:
                break
        page.evaluate("() => window.scrollBy(0, document.body.scrollHeight)")
        time.sleep(1.0 + random.random()*0.6)
        tries += 1
    return list(urls)

def extract_host_block(page):
    # Cherche une section “Meet your host / Aller à la rencontre…”
    name = rating = joined = ""
    try:
        # large body text
        body = page_text(page)

        # Nom de l’hôte: souvent “Hosted by <Name>”
        m = re.search(r"(?:Hosted by|Propos[eé] par|Par l[’']h[oô]te)\s+([A-Z][A-Za-zÀ-ÖØ-öø-ÿ' -]{1,50})", body, re.IGNORECASE)
        if m:
            name = norm(m.group(1))

        # Variante: heading proche de “Meet your host”
        for lbl in HOST_SECTION_LABELS:
            sec = page.get_by_text(lbl, exact=False)
            if sec.count():
                # regarde quelques éléments voisins
                root = sec.first
                # parent chain then find a name-like node
                try:
                    block = root.locator("xpath=..").first
                except Exception:
                    block = root
                # cherche un motif de nom capitalisé
                for cand in block.locator("h2, h3, a, span, div").all():
                    try:
                        t = norm(cand.inner_text(timeout=300))
                        if 2 <= len(t) <= 60 and re.match(r"^[A-ZÀ-ÖØ-Þ][A-Za-zÀ-ÖØ-öø-ÿ' -]+$", t):
                            name = name or t
                    except Exception:
                        continue
        # Note globale ou “x reviews”
        m2 = re.search(r"([0-5]\.[0-9])\s*(?:average|overall)?\s*rating", body, re.IGNORECASE)
        if m2: rating = m2.group(1)
        if not rating:
            # motif “Rated 4.95 out of 5” ou “4,95 · 120 reviews”
            m3 = re.search(r"Rated\s*([0-5][\.,][0-9]{1,2})\s*out of\s*5", body, re.IGNORECASE)
            if m3: rating = m3.group(1).replace(",", ".")
        if not rating:
            m4 = re.search(r"\b([0-5][\.,][0-9]{1,2})\b\s*[·•]\s*\d+\s*reviews", body, re.IGNORECASE)
            if m4: rating = m4.group(1).replace(",", ".")

        # “Joined in 2020” / “Hôte depuis 2019” / “Hosting since 2018”
        for pat in [
            r"Joined in\s+([A-Za-z]+\s+\d{4}|\d{4})",
            r"H[oô]te depuis\s+([A-Za-z]+\s+\d{4}|\d{4})",
            r"Hosting since\s+([A-Za-z]+\s+\d{4}|\d{4})",
            r"Exerce depuis\s+([A-Za-z]+\s+\d{4}|\d{4})",
        ]:
            m5 = re.search(pat, body, re.IGNORECASE)
            if m5:
                joined = norm(m5.group(1))
                break
    except Exception:
        pass
    return name, rating, joined

def extract_from_json(page):
    # Essaie __NEXT_DATA__ / LD+JSON pour la note hôte
    host_rating = ""
    try:
        node = page.locator("script#__NEXT_DATA__")
        if node.count():
            j = json.loads(node.first.inner_text(timeout=1200))
            txt = json.dumps(j)
            m = re.search(r'"hostOverallRating"\s*:\s*([0-9.]+)', txt)
            if m:
                host_rating = m.group(1)
    except Exception:
        pass

    # LD+JSON
    if not host_rating:
        try:
            for i in range(page.locator("script[type='application/ld+json']").count()):
                raw = page.locator("script[type='application/ld+json']").nth(i).inner_text(timeout=800)
                data = json.loads(raw)
                txt = json.dumps(data)
                m = re.search(r'"hostOverallRating"\s*:\s*([0-9.]+)', txt)
                if m:
                    host_rating = m.group(1)
                    break
        except Exception:
            pass
    return host_rating

def extract_listing(page):
    def first_text(sel, timeout=1500):
        loc = page.locator(sel)
        if not loc.count(): return ""
        try:
            return norm(loc.first.inner_text(timeout=timeout))
        except Exception:
            return ""

    # Titre
    title = first_text("h1")

    # Prix si dates fournies
    price = ""
    pb = page.locator("[data-testid='book-it-default']")
    if pb.count():
        try:
            price = norm(pb.first.inner_text(timeout=1500))
        except Exception:
            price = ""
    if not price:
        t = first_text("div:has-text('AED')")
        price = "" if "Add dates for prices" in t else t

    # Déplie “Afficher plus / Show more”
    click_all_show_more(page)
    time.sleep(0.2)

    # Code licence
    full_txt = page_text(page)
    license_code = find_license_in_text(full_txt)

    # Bloc hôte
    host_name, host_overall, host_joined = extract_host_block(page)
    # JSON fallback pour la note hôte
    if not host_overall:
        host_overall = extract_from_json(page)

    return {
        "title": title,
        "price": price,
        "license_code": license_code,
        "host_name": host_name,
        "host_overall_rating": host_overall,
        "host_joined": host_joined
    }

# --------- Main ----------
def main():
    deadline = datetime.utcnow() + timedelta(minutes=MAX_MINUTES)
    proxy_cfg = {"server": PROXY} if PROXY else None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA, locale="fr-FR,en-US", proxy=proxy_cfg)
        page = ctx.new_page()
        page.set_default_timeout(20000)

        # Page de recherche
        try:
            page.goto(START_URL, wait_until="domcontentloaded")
        except PWTimeout:
            pass

        # Collecte des URLs
        listing_urls = collect_listing_urls(page, MAX_LIST, deadline)

        # CSV
        header = [
            "url",
            "title",
            "license_code",
            "host_name",
            "host_overall_rating",
            "host_joined",           # “Joined in …” / “Hôte depuis …”
            "price_text",
            "scraped_at"
        ]
        new_file = not os.path.exists(OUTPUT_CSV)
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig") as fh:
            w = csv.writer(fh)
            if new_file: w.writerow(header)

            count = 0
            for base_url in listing_urls:
                if datetime.utcnow() >= deadline: break
                url = build_listing_url(base_url)
                try:
                    page.goto(url, wait_until="domcontentloaded")
                except PWTimeout:
                    continue
                time.sleep(0.9 + random.random()*0.8)

                data = extract_listing(page)
                w.writerow([
                    base_url,
                    data["title"],
                    data["license_code"],
                    data["host_name"],
                    data["host_overall_rating"],
                    data["host_joined"],
                    data["price"],
                    datetime.utcnow().isoformat()
                ])

                count += 1
                if count >= MAX_LIST: break
                time.sleep(1.0 + random.random())

        browser.close()

if __name__ == "__main__":
    main()
