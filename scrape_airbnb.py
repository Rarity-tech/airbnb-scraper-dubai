# scrape_airbnb.py
import asyncio, csv, os, re, sys
from datetime import datetime
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL    = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "50"))
PROXY        = os.getenv("PROXY", "").strip()

ROOM_URL_RE = re.compile(r"/rooms/\d+")

# --------- regex robustes ---------
LICENSE_PATTERNS = [
    # EN
    r"\b(license|licence)\s*(number|no\.?)?\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
    r"\b(registration)\s*(number|no\.?)?\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
    r"\b(permit)\s*(number|no\.?)?\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
    r"\b(dtcm|dubai\s*tourism)\s*(permit|license|licence)?\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
    # FR
    r"\b(num[eé]ro\s+d['’]enregistrement)\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
    r"\b(enregistrement\s*(?:n[ou]m[eé]ro)?)\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
    r"\b(num[eé]ro\s+de\s+licen[cs]e)\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
]
HOST_JOINED_PAT = re.compile(r"(Joined in|A rejoint Airbnb en)\s+(\d{4})", re.I)
RATING_PATTERNS = [
    r"(Average rating|Host rating|Note moyenne|Note de l['’]h[ôo]te)\s*[:\s]\s*(\d\.\d)",
    r"\b(\d\.\d)\s*[·•]\s*\d+\s+(reviews|avis)\b",
]

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def clean_license(raw: str) -> str:
    if not raw:
        return ""
    raw = re.sub(r"^(?i)(license|licence|registration|permit|dtcm|dubai tourism|num[eé]ro.*|enregistrement)\s*(number|no\.?)?\s*[:\-]?\s*", "", raw)
    raw = re.split(r"[\n\r|•·;]", raw)[0]
    return raw.strip(" .:-")

async def click_cookies(page):
    for label in ["Accept", "Agree", "OK", "Tout accepter", "Autoriser", "J'accepte"]:
        try:
            await page.get_by_role("button", name=re.compile(label, re.I)).click(timeout=1500)
            break
        except Exception:
            pass

# --------- collecte des URLs ---------
async def collect_listing_urls(page, max_urls: int):
    urls, seen, stagnant = [], set(), 0
    while len(urls) < max_urls and stagnant < 8:
        for a in await page.query_selector_all("a[href*='/rooms/']"):
            href = await a.get_attribute("href")
            if not href:
                continue
            if href.startswith("/"):
                href = urljoin("https://www.airbnb.com", href)
            try:
                path = urlparse(href).path or ""
            except Exception:
                continue
            m = ROOM_URL_RE.search(path)
            if not m:
                continue
            u = "https://www.airbnb.com" + m.group(0)
            if u not in seen:
                seen.add(u); urls.append(u)
                if len(urls) >= max_urls: break
        if len(urls) < max_urls:
            await page.mouse.wheel(0, 2200)
            await page.wait_for_timeout(800)
            stagnant += 1
        else:
            break
    return urls[:max_urls]

# --------- extraction stricte ---------
async def find_host_card(page):
    # 1) section “Hosted by / Hébergé par”
    candidates = [
        "section:has(h2:has-text('Hosted by'))",
        "section:has(h2:has-text('Hébergé par'))",
        "section:has(h2:has-text('Hôte'))",
        "[data-section-id='HOST_PROFILE_DEFAULT']",
    ]
    for sel in candidates:
        loc = page.locator(sel).first
        if await loc.count():
            return loc
    # 2) carte hôte proche d’un bouton “Contact” (évite la zone Avis)
    loc = page.locator("section:not([aria-label*='Review']):has(a[href*='/users/show/'])").first
    return loc if await loc.count() else None

async def extract_listing(page, url: str):
    row = {
        "url": url, "title": "", "license_code": "",
        "host_name": "", "host_overall_rating": "",
        "host_profile_url": "", "host_joined": "",
        "scraped_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await click_cookies(page)

        # titre
        try:
            t = await page.locator("h1[data-testid='title'], h1").first.text_content()
            row["title"] = norm(t)
        except Exception:
            pass

        # bloc hôte uniquement, jamais dans Avis
        host_card = await find_host_card(page)
        if host_card:
            # nom
            try:
                # nom apparaît souvent dans le h2 “Hosted by {name}”
                h2 = host_card.locator("h2").first
                txt = norm(await h2.text_content() or "")
                m = re.search(r"(Hosted by|Hébergé par|Hôte[:\s]*)\s*(.*)$", txt, re.I)
                row["host_name"] = norm(m.group(2)) if m else txt
            except Exception:
                pass
            # lien profil hôte
            try:
                a = host_card.locator("a[href*='/users/show/']").first
                if await a.count():
                    href = await a.get_attribute("href")
                    row["host_profile_url"] = urljoin("https://www.airbnb.com", href or "")
            except Exception:
                pass

        # année “Joined”
        try:
            body_txt = await page.inner_text("body", timeout=8000)
            m = HOST_JOINED_PAT.search(body_txt or "")
            if m: row["host_joined"] = m.group(2)
        except Exception:
            pass

        # licence: déplier et chercher partout, plusieurs langues
        try:
            for btn in ["Show more", "Voir plus", "Mehr anzeigen", "Ver más", "Afficher plus"]:
                try: await page.get_by_role("button", name=re.compile(btn, re.I)).click(timeout=1200)
                except Exception: pass
            body_txt = await page.inner_text("body", timeout=8000)
            lic = ""
            for pat in LICENSE_PATTERNS:
                m = re.search(pat, body_txt, flags=re.I)
                if m:
                    lic = m.group(m.lastindex or 0)
                    break
            row["license_code"] = clean_license(lic)
        except Exception:
            pass

        # profil hôte pour la note globale hôte
        if row["host_profile_url"]:
            try:
                await page.goto(row["host_profile_url"], wait_until="domcontentloaded", timeout=60000)
                await click_cookies(page)
                prof = await page.inner_text("body", timeout=8000)
                if not row["host_joined"]:
                    m = HOST_JOINED_PAT.search(prof or "")
                    if m: row["host_joined"] = m.group(2)
                rating = ""
                for pat in RATING_PATTERNS:
                    m = re.search(pat, prof or "", flags=re.I)
                    if m:
                        rating = m.group(m.lastindex or 0)
                        break
                if not rating:
                    m = re.search(r"\b(\d\.\d)\b(?=[^\n]{0,30}(reviews|avis))", prof or "", flags=re.I)
                    if m: rating = m.group(1)
                row["host_overall_rating"] = rating
            except Exception:
                pass

    except Exception as e:
        print("LISTING_ERROR", url, repr(e))
    return row

async def main():
    pw = await async_playwright().start()
    args = ["--disable-blink-features=AutomationControlled"]
    browser = await pw.chromium.launch(headless=True, args=args, proxy={"server": PROXY} if PROXY else None)
    context = await browser.new_context(
        viewport={"width": 1366, "height": 2200},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        locale="en-US",  # libellés gérés EN/FR dans le code
    )
    page = await context.new_page()

    try:
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
    except PWTimeout:
        await page.goto(START_URL.replace("/homes", "/stays"), wait_until="domcontentloaded", timeout=60000)

    await click_cookies(page)
    urls = await collect_listing_urls(page, MAX_LISTINGS)
    print(f"FOUND_URLS {len(urls)}")

    rows = []
    for i, u in enumerate(urls, 1):
        r = await extract_listing(page, u)
        rows.append(r)
        print(f"[{i}/{len(urls)}] {r['url']} | host={r['host_name']} | lic={r['license_code']} | rating={r['host_overall_rating']}")

    await browser.close(); await pw.stop()

    with open("airbnb_results.csv", "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "url","title","license_code","host_name",
            "host_overall_rating","host_profile_url","host_joined","scraped_at",
        ])
        writer.writeheader(); writer.writerows(rows)
    print(f"SAVED {len(rows)} rows to airbnb_results.csv")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
