# scrape_airbnb.py
import os, csv, re, time, datetime
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.getenv("MAX_LISTINGS", "20"))
MAX_MINUTES = float(os.getenv("MAX_MINUTES", "5"))
PROXY       = os.getenv("PROXY", "").strip() or None

# ---------- helpers ----------

def now_iso():
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

def write_csv(rows, path="airbnb_results.csv"):
    header = [
        "url","title","license_code",
        "host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"
    ]
    # UTF-8 with BOM so Excel shows accents correctly
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def click_if_present(page, selector, timeout=2000):
    try:
        el = page.locator(selector).first
        if el.is_visible(timeout=timeout):
            el.click()
            return True
    except PWTimeout:
        pass
    except Exception:
        pass
    return False

def get_text_safe(loc):
    try:
        return loc.inner_text(timeout=2000).strip()
    except Exception:
        return ""

# ---------- URL collection ----------

def collect_listing_urls(page, max_items, max_minutes):
    page.goto(START_URL, wait_until="networkidle")
    start = time.time()
    seen = set()
    last_h = 0

    # accept cookies if present
    click_if_present(page, 'button:has-text("Accepter")')
    click_if_present(page, 'button:has-text("I agree")')
    click_if_present(page, 'button:has-text("OK")')

    while len(seen) < max_items and (time.time() - start) < (max_minutes * 60):
        # collect anchors that look like listings
        anchors = page.locator('a[href^="/rooms/"]').all()
        for a in anchors:
            try:
                href = a.get_attribute("href") or ""
                if not href:
                    continue
                # ignore fragment anchors to reviews/photos etc.
                if any(x in href for x in ["experiences", "things-to-do"]):
                    continue
                # normalize to absolute fr/ww domain
                full = urljoin(page.url, href.split("?")[0])
                if "/rooms/" in full:
                    seen.add(full)
                    if len(seen) >= max_items:
                        break
            except Exception:
                continue

        # infinite scroll
        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        page.wait_for_timeout(600)
        # stop if no more scroll
        h = page.evaluate("document.body.scrollHeight")
        if h == last_h:
            break
        last_h = h

    urls = list(seen)[:max_items]
    print(f"FOUND_URLS {len(urls)}")
    for i,u in enumerate(urls,1):
        print(f"#{i} {u}")
    return urls

# ---------- PDP parsing ----------

RE_LICENSES = [
    re.compile(r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6}\b"),  # e.g. BUR-BUR-OFXPS
    re.compile(r"\b\d{5,8}\b"),                          # e.g. 1100042
    re.compile(r"\b[A-Z0-9]{5,}\b"),                     # fallback
]

LABEL_PATTERNS = [
    "Infos d'enregistrement",
    "Détails de l'enregistrement",
    "Registration details",
    "License", "Licence", "Permit"
]

def extract_license_code(page):
    # open the about dialog if present
    opened = (
        click_if_present(page, 'button:has-text("Lire la suite")') or
        click_if_present(page, 'span:has-text("Lire la suite")') or
        click_if_present(page, 'button:has-text("Afficher plus")') or
        click_if_present(page, 'button:has-text("Read more")')
    )
    text_scope = ""
    if opened:
        # wait for modal
        try:
            dlg = page.locator('[role="dialog"], [aria-modal="true"]').first
            dlg.wait_for(state="visible", timeout=3000)
            text_scope = get_text_safe(dlg)
        except Exception:
            text_scope = ""
    if not text_scope:
        # fallback to whole page text
        text_scope = get_text_safe(page.locator("body"))

    # narrow to section following the label if we can
    if any(lbl in text_scope for lbl in LABEL_PATTERNS):
        # take substring starting at first label occurrence
        for lbl in LABEL_PATTERNS:
            idx = text_scope.find(lbl)
            if idx >= 0:
                text_scope = text_scope[idx: idx + 800]  # scan next chunk only
                break

    for rx in RE_LICENSES:
        m = rx.search(text_scope)
        if m:
            return m.group(0)
    return ""

def extract_host_block(page):
    # Prefer the host section; avoids reviewer links.
    candidates = [
        'section[data-section-id^="HOST"]',
        'section:has-text("Hôte")',
        'section:has-text("Hosted by")',
        '[data-plugin-in-point-id="ABOUT_HOST"]'
    ]
    for sel in candidates:
        loc = page.locator(sel).first
        if loc.count() and loc.is_visible():
            return loc
    return page.locator("body")

def extract_host_info(page):
    host_block = extract_host_block(page)
    # profile URL inside host block only
    profile_a = host_block.locator('a[href^="/users/show/"]').first
    profile_url = ""
    try:
        href = profile_a.get_attribute("href")
        if href:
            profile_url = urljoin(page.url, href)
    except Exception:
        profile_url = ""

    # host name: try aria-label on the same link
    host_name = ""
    try:
        aria = profile_a.get_attribute("aria-label") or ""
        # e.g. "Profil de Samir" / "Profile of Anna"
        m = re.search(r"(?:Profil de|Profile of)\s+(.+)", aria)
        if m:
            host_name = m.group(1).strip()
    except Exception:
        pass

    if not host_name:
        # try heading text that contains "Hôte"
        txt = get_text_safe(host_block)
        m = re.search(r"H[oô]te\s*:?\s*([^\n•]+)", txt)
        if m:
            host_name = m.group(1).strip()

    # joined year
    host_joined = ""
    m = re.search(r"H[oô]te depuis\s+(\d{4})", get_text_safe(host_block))
    if m:
        host_joined = m.group(1)

    # overall rating near host block
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
        page.goto(url, wait_until="domcontentloaded", timeout=45000)
        # Some pages need a short settle time
        page.wait_for_timeout(800)

        # title
        title = get_text_safe(page.locator('h1[data-testid="title"]'))
        if not title:
            title = get_text_safe(page.locator("h1"))
        data["title"] = title

        # host info
        hn, hp, hr, hj = extract_host_info(page)
        data["host_name"] = hn
        data["host_profile_url"] = hp
        data["host_overall_rating"] = hr
        data["host_joined"] = hj

        # license code
        data["license_code"] = extract_license_code(page)

    except Exception as e:
        print(f"ERROR parsing {url}: {e}")
    return data

# ---------- main ----------

def main():
    rows = []
    with sync_playwright() as p:
        launch_args = {"headless": True}
        if PROXY:
            launch_args["proxy"] = {"server": PROXY}
        browser = p.chromium.launch(**launch_args)
        context = browser.new_context(locale="fr-FR")
        page = context.new_page()

        urls = collect_listing_urls(page, MAX_LIST, MAX_MINUTES)
        for u in urls:
            try:
                rows.append(parse_listing(page, u))
            except Exception as e:
                print(f"ERROR listing {u}: {e}")

        write_csv(rows)
        print(f"SAVED {len(rows)} rows to airbnb_results.csv")

        context.close()
        browser.close()

if __name__ == "__main__":
    main()
