# scrape_airbnb.py
import os, csv, re, time, datetime
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.getenv("MAX_LISTINGS", "20"))
MAX_MINUTES = float(os.getenv("MAX_MINUTES", "5"))
PROXY       = os.getenv("PROXY", "").strip() or None
OUT_CSV     = "airbnb_results.csv"

# ---------------- utils ----------------

def now_iso():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

def write_csv(rows, path=OUT_CSV):
    header = [
        "url","title","license_code",
        "host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def click_if_present(page, selector, timeout=3000):
    try:
        el = page.locator(selector).first
        el.wait_for(state="visible", timeout=timeout)
        el.click()
        return True
    except Exception:
        return False

def get_text_safe(loc, timeout=2500):
    try:
        return loc.inner_text(timeout=timeout).strip()
    except Exception:
        return ""

# ---------------- navigation ----------------

def goto_search_with_retry(page):
    # Préfère le domaine fr pour limiter redirections.
    candidates = []
    if "fr.airbnb.com" in START_URL:
        candidates = [START_URL, START_URL.replace("fr.airbnb.com","www.airbnb.com")]
    else:
        candidates = [START_URL.replace("www.airbnb.com","fr.airbnb.com"), START_URL]

    last_err = None
    for url in candidates:
        for _ in range(2):
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                # cookies
                click_if_present(page, 'button:has-text("Accepter")', 4000) or \
                click_if_present(page, 'button:has-text("I agree")', 4000) or \
                click_if_present(page, 'button:has-text("OK")', 4000)
                # attend qu’au moins une carte soit chargée
                page.wait_for_selector('a[href^="/rooms/"]', timeout=30000)
                return
            except Exception as e:
                last_err = e
                try:
                    page.reload(wait_until="domcontentloaded", timeout=30000)
                except Exception:
                    pass
    raise last_err if last_err else RuntimeError("navigation failed")

# ---------------- collecte URLs ----------------

def collect_listing_urls(page, max_items, max_minutes):
    goto_search_with_retry(page)

    start = time.time()
    seen = set()
    last_h = 0

    while len(seen) < max_items and (time.time() - start) < (max_minutes * 60):
        for a in page.locator('a[href^="/rooms/"]').all():
            try:
                href = a.get_attribute("href") or ""
                if not href or "experiences" in href:
                    continue
                full = urljoin(page.url, href.split("?")[0])
                if "/rooms/" in full:
                    seen.add(full)
                    if len(seen) >= max_items:
                        break
            except Exception:
                continue

        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        page.wait_for_timeout(700)
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

    urls = list(seen)[:max_items]
    print(f"FOUND_URLS {len(urls)}")
    for i,u in enumerate(urls,1):
        print(f"#{i} {u}")
    return urls

# ---------------- LICENSE (inchangé) ----------------

RE_LICENSES = [
    re.compile(r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}\b"),
    re.compile(r"\b\d{5,8}\b"),
    re.compile(r"\b[A-Z0-9]{5,}\b"),
]
LABEL_PATTERNS = [
    "Infos d'enregistrement","Détails de l'enregistrement",
    "Registration details","License","Licence","Permit"
]

def extract_license_code(page):
    opened = (
        click_if_present(page, 'button:has-text("Lire la suite")') or
        click_if_present(page, 'span:has-text("Lire la suite")') or
        click_if_present(page, 'button:has-text("Afficher plus")') or
        click_if_present(page, 'button:has-text("Read more")')
    )
    text_scope = ""
    if opened:
        try:
            dlg = page.locator('[role="dialog"], [aria-modal="true"]').first
            dlg.wait_for(state="visible", timeout=3000)
            text_scope = get_text_safe(dlg, timeout=3000)
        except Exception:
            pass
    if not text_scope:
        text_scope = get_text_safe(page.locator("body"), timeout=6000)

    if any(lbl in text_scope for lbl in LABEL_PATTERNS):
        for lbl in LABEL_PATTERNS:
            i = text_scope.find(lbl)
            if i >= 0:
                text_scope = text_scope[i:i+800]
                break

    for rx in RE_LICENSES:
        m = rx.search(text_scope)
        if m:
            return m.group(0)
    return ""

# ---------------- HOST (depuis bloc “Faites connaissance…”) ----------------

def find_host_section(page):
    candidates = [
        'section:has(h2:has-text("Faites connaissance avec votre hôte"))',
        'section:has(h2:has-text("Meet your Host"))',
        'section:has(h2:has-text("Get to know your host"))',
        'section:has(h2:has-text("Conoce a tu anfitri"))',
        'section:has(h2:has-text("Erfahre mehr über deinen Gastgeber"))',
    ]
    for sel in candidates:
        loc = page.locator(sel)
        try:
            if loc.count() and loc.first.is_visible():
                return loc.first
        except Exception:
            continue
    return None

def extract_host_fields(page, listing_url):
    host_name = host_overall_rating = host_profile_url = host_joined = ""
    # scroll vers le bas pour charger le bloc hôte
    for _ in range(6):
        page.mouse.wheel(0, 1400)
        page.wait_for_timeout(250)

    sect = find_host_section(page)
    if not sect:
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
            if href:
                host_profile_url = urljoin(listing_url, href.split("?")[0])
    except Exception:
        link = None

    # Nom de l’hôte
    try:
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
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*[★*]", block) \
        or re.search(r"Note globale\s*:?[\s\n]*([0-9]+(?:[.,][0-9]+)?)", block, re.I) \
        or re.search(r"(\d+(?:[.,]\d+)?)\s*[•·]\s*(?:avis|reviews)", block, re.I)
    if m:
        host_overall_rating = m.group(1).replace(",", ".")

    # Année/mois depuis quand sur Airbnb
    m2 = re.search(r"(depuis|since)\s+(?:\w+\s+)?(\d{4})", block, re.I)
    if m2:
        host_joined = m2.group(2)

    return host_name, host_overall_rating, host_profile_url, host_joined

# ---------------- parsing PDP ----------------

def parse_listing(page, url):
    data = {
        "url": url, "title": "", "license_code": "",
        "host_name": "", "host_overall_rating": "",
        "host_profile_url": "", "host_joined": "", "scraped_at": now_iso()
    }
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(600)

        title = (
            page.locator('meta[property="og:title"]').first.get_attribute("content") or
            get_text_safe(page.locator('h1[data-testid="title"]')) or
            get_text_safe(page.locator("h1"))
        )
        data["title"] = title

        # Host via bloc dédié
        hn, hr, hp, hj = extract_host_fields(page, url)
        data.update({"host_name": hn, "host_profile_url": hp, "host_overall_rating": hr, "host_joined": hj})

        # Licence via méthode inchangée
        data["license_code"] = extract_license_code(page)

    except Exception as e:
        print(f"ERROR parsing {url}: {e}")
    return data

# ---------------- main ----------------

def main():
    rows = []
    with sync_playwright() as p:
        launch_args = {"headless": True}
        if PROXY:
            launch_args["proxy"] = {"server": PROXY}
        browser = p.chromium.launch(**launch_args)
        context = browser.new_context(
            locale="fr-FR",
            user_agent=("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"),
            viewport={"width":1280,"height":1600},
            timezone_id="Europe/Paris",
        )
        page = context.new_page()

        urls = collect_listing_urls(page, MAX_LIST, MAX_MINUTES)
        for u in urls:
            rows.append(parse_listing(page, u))

        write_csv(rows)
        print(f"SAVED {len(rows)} rows to {OUT_CSV}")

        context.close()
        browser.close()

if __name__ == "__main__":
    main()
