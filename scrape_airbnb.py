# scrape_airbnb.py
import asyncio, csv, os, re, sys
from datetime import datetime
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL    = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "50"))
MAX_MINUTES  = int(os.getenv("MAX_MINUTES", "10"))
PROXY        = os.getenv("PROXY", "").strip()  # format: http://user:pass@host:port

# ---------------------------
# Helpers robustes
# ---------------------------

LICENSE_PATTERNS = [
    r"(license\s*(number|no\.?)\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
    r"(num[eé]ro\s+de\s+licen[cs]e\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
    r"(registration\s*(number|no\.?)\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
    r"(permit\s*(number|no\.?)\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
    r"(dtcm\s*(permit|license)\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
    r"(dubai\s*tourism\s*(permit|license)\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
    r"(num[eé]ro\s+d['’]enregistrement\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
    r"(enregistrement\s*n[ou]m[eé]ro\s*:?[\s\n]*)([A-Za-z0-9\-_/\. ]{3,})",
]

HOST_JOINED_PAT = re.compile(r"(Joined in|A rejoint Airbnb en)\s+(\d{4})", re.I)
RATING_PATTERNS = [
    # profil hôte en anglais
    r"(Average rating|Host rating)\s*[:\s]\s*(\d\.\d)",
    # formats compacts "4.9 · 128 reviews" mais sur le PROFIL
    r"\b(\d\.\d)\s*[·•]\s*\d+\s+(reviews|avis)\b",
    # français
    r"(Note moyenne|Note de l['’]h[ôo]te)\s*[:\s]\s*(\d\.\d)",
]

ROOM_URL_RE = re.compile(r"/rooms/\d+")

def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def extract_first(patterns, text):
    for pat in patterns:
        m = re.search(pat, text, flags=re.I)
        if not m:
            continue
        # dernière capture supposée contenir la valeur
        val = m.group(m.lastindex) if m.lastindex else m.group(0)
        return normalize_whitespace(val)
    return ""

def clean_license(lic: str) -> str:
    if not lic:
        return ""
    # supprimer préfixes textuels résiduels
    lic = re.sub(r"^(license|num[eé]ro.*licen[cs]e|registration|permit|dtcm|dubai tourism|enregistrement)\s*(number|no\.?)?\s*[:\-]?\s*", "", lic, flags=re.I)
    lic = normalize_whitespace(lic)
    # couper aux sauts ou ponctuation forte
    lic = re.split(r"[|·•\n\r]", lic)[0]
    return lic.strip(" :.-")

# ---------------------------
# Scraping
# ---------------------------

async def collect_listing_urls(page, max_urls: int):
    urls, seen, stagnant = [], set(), 0
    while len(urls) < max_urls and stagnant < 8:
        anchors = await page.query_selector_all("a[href*='/rooms/']")
        added = 0
        for a in anchors:
            href = await a.get_attribute("href")
            if not href:
                continue
            # rendre absolu
            if href.startswith("/"):
                href = urljoin("https://www.airbnb.com", href)
            # nettoyer query
            try:
                path = urlparse(href).path
            except Exception:
                continue
            m = ROOM_URL_RE.search(path or "")
            if not m:
                continue
            room_url = "https://www.airbnb.com" + m.group(0)
            if room_url not in seen:
                seen.add(room_url)
                urls.append(room_url)
                added += 1
                if len(urls) >= max_urls:
                    break
        if added == 0:
            stagnant += 1
        else:
            stagnant = 0
        await page.mouse.wheel(0, 2200)
        await page.wait_for_timeout(750)
    return urls[:max_urls]

async def click_cookies(page):
    try:
        await page.get_by_role("button", name=re.compile("Accept|Agree|OK|Tout accepter|Autoriser", re.I)).click(timeout=3000)
    except Exception:
        pass

async def extract_from_listing(page, url: str):
    row = {
        "url": url,
        "title": "",
        "license_code": "",
        "host_name": "",
        "host_overall_rating": "",
        "host_profile_url": "",
        "host_joined": "",
        "scraped_at": datetime.utcnow().isoformat(timespec="seconds"),
    }
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await click_cookies(page)

        # Titre
        try:
            t = await page.locator("h1[data-testid='title'], h1").first.text_content()
            row["title"] = normalize_whitespace(t)
        except Exception:
            pass

        # Lien profil hôte + nom
        host_link = None
        try:
            host_link = page.locator("a[data-testid='user-profile-link']").first
            if not await host_link.count():
                host_link = page.locator("a[href*='/users/show/']").first
            if await host_link.count():
                href = await host_link.get_attribute("href")
                row["host_profile_url"] = urljoin("https://www.airbnb.com", href or "")
                row["host_name"] = normalize_whitespace(await host_link.text_content() or "")
        except Exception:
            pass

        # Année d'inscription (dans le bloc hôte de l’annonce)
        try:
            txt = await page.inner_text("body", timeout=10000)
            m = HOST_JOINED_PAT.search(txt or "")
            if m:
                row["host_joined"] = m.group(2)
        except Exception:
            pass

        # Numéro de licence: chercher sur toute la page
        try:
            full = await page.inner_text("body", timeout=10000)
            lic = extract_first(LICENSE_PATTERNS, full or "")
            row["license_code"] = clean_license(lic)
        except Exception:
            pass

        # Si besoin, tenter section “About this place” / “À propos”
        if not row["license_code"]:
            try:
                # ouvrir “Voir plus” pour déplier
                for btn_text in ["Show more", "Voir plus", "Mehr anzeigen", "Ver más"]:
                    try:
                        await page.get_by_role("button", name=re.compile(btn_text, re.I)).click(timeout=1500)
                    except Exception:
                        pass
                full = await page.inner_text("body", timeout=8000)
                lic = extract_first(LICENSE_PATTERNS, full or "")
                row["license_code"] = clean_license(lic)
            except Exception:
                pass

        # Note d'hôte: ouvrir le profil et extraire là-bas
        if row["host_profile_url"]:
            try:
                # ouvrir profil dans le même onglet pour limiter l’empreinte
                await page.goto(row["host_profile_url"], wait_until="domcontentloaded", timeout=60000)
                await click_cookies(page)
                prof_txt = await page.inner_text("body", timeout=10000)
                # année d'inscription depuis le profil si absente
                if not row["host_joined"]:
                    m = HOST_JOINED_PAT.search(prof_txt or "")
                    if m:
                        row["host_joined"] = m.group(2)
                # note d'hôte
                host_rating = extract_first(RATING_PATTERNS, prof_txt or "")
                if not host_rating:
                    # fallback: première note décimale entourée de "reviews/avis" sur profil
                    m = re.search(r"\b(\d\.\d)\b(?=[^\n]{0,30}(reviews|avis))", prof_txt or "", flags=re.I)
                    if m:
                        host_rating = m.group(1)
                row["host_overall_rating"] = host_rating
            except Exception:
                pass

    except Exception as e:
        print("LISTING_ERROR", url, repr(e))
    return row

async def main():
    pw = await async_playwright().start()
    launch_args = ["--disable-blink-features=AutomationControlled"]
    proxy_cfg = {"server": PROXY} if PROXY else None
    browser = await pw.chromium.launch(headless=True, args=launch_args, proxy=proxy_cfg)
    context = await browser.new_context(
        viewport={"width": 1366, "height": 2200},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        locale="en-US",  # langue stable
        java_script_enabled=True,
    )
    page = await context.new_page()

    # Page résultats: pas de dates, simple scroll
    try:
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=60000)
    except PWTimeout:
        # fallback vers /stays
        await page.goto(START_URL.replace("/homes", "/stays"), wait_until="domcontentloaded", timeout=60000)

    await click_cookies(page)
    urls = await collect_listing_urls(page, MAX_LISTINGS)
    print(f"FOUND_URLS {len(urls)}")

    results = []
    for i, u in enumerate(urls, 1):
        row = await extract_from_listing(page, u)
        results.append(row)
        print(f"[{i}/{len(urls)}] {row['url']} | host={row['host_name']} | lic={row['license_code']} | rating={row['host_overall_rating']}")
    await browser.close()
    await pw.stop()

    # Écriture CSV sans prix
    with open("airbnb_results.csv", "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "url",
                "title",
                "license_code",
                "host_name",
                "host_overall_rating",
                "host_profile_url",
                "host_joined",
                "scraped_at",
            ],
        )
        writer.writeheader()
        writer.writerows(results)
    print(f"SAVED {len(results)} rows to airbnb_results.csv")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
