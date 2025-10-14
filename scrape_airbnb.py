# scrape_airbnb.py
import os, csv, re, time, datetime
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.getenv("MAX_LISTINGS", "20"))
MAX_MINUTES = float(os.getenv("MAX_MINUTES", "5"))
PROXY       = os.getenv("PROXY", "").strip() or None

# ---------------- utils ----------------

def now_iso():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

def write_csv(rows, path="airbnb_results.csv"):
    header = [
        "url","title","license_code",
        "host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"
    ]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:  # Excel-safe accents
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

def get_text_safe(loc):
    try:
        return loc.inner_text(timeout=2500).strip()
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
                # attend qu'au moins une carte soit chargée
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

# ---------------- parsing PDP ----------------

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
            text_scope = get_text_safe(dlg)
        except Exception:
            pass
    if not text_scope:
        text_scope = get_text_safe(page.locator("body"))

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

def extract_host_block(page):
    for sel in [
        'section[data-section-id^="HOST"]',
        'section:has-text("Hôte")',
        'section:has-text("Hosted by")',
        '[data-plugin-in-point-id="ABOUT_HOST"]'
    ]:
        loc = page.locator(sel).first
        try:
            if loc.count() and loc.is_visible():
                return loc
        except Exception:
            continue
    return page.locator("body")

def extract_host_info(page):
    host_block = extract_host_block(page)

    profile_a = host_block.locator('a[href^="/users/show/"]').first
    profile_url = ""
    try:
        href = profile_a.get_attribute("href")
        if href:
            profile_url = urljoin(page.url, href)
    except Exception:
        pass

    host_name = ""
    try:
        aria = profile_a.get_attribute("aria-label") or ""
        m = re.search(r"(?:Profil de|Profile of)\s+(.+)", aria)
        if m:
            host_name = m.group(1).strip()
    except Exception:
        pass
    if not host_name:
        txt = get_text_safe(host_block)
        m = re.search(r"H[oô]te\s*:?\s*([^\n•]+)", txt)
        if m:
            host_name = m.group(1).strip()

    host_joined = ""
    m = re.search(r"H[oô]te depuis\s+(\d{4})", get_text_safe(host_block))
    if m:
        host_joined = m.group(1)

    host_rating = ""
    m = re.search(r"(\d[\d.,]*)\s*/\s*5", get_text_safe(host_block))
    if m:
        host_rating = m.group(1).replace(",", ".")

    return host_name, profile_url, host_rating, host_joined

def parse_listing(page, url):
    data = {
        "url": url, "title": "", "license_code": "",
        "host_name": "", "host_overall_rating": "",
        "host_profile_url": "", "host_joined": "", "scraped_at": now_iso()
    }
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(600)

        title = get_text_safe(page.locator('h1[data-testid="title"]')) or get_text_safe(page.locator("h1"))
        data["title"] = title

        hn, hp, hr, hj = extract_host_info(page)
        data.update({"host_name": hn, "host_profile_url": hp, "host_overall_rating": hr, "host_joined": hj})

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
        print(f"SAVED {len(rows)} rows to airbnb_results.csv")

        context.close()
        browser.close()

if __name__ == "__main__":
    main()
