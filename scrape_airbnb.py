# filename: scrape_airbnb.py
# deps:
#   pip install playwright
#   python -m playwright install --with-deps chromium
#
# ENV support:
#   START_URL=https://www.airbnb.com/s/Dubai/homes
#   MAX_LISTINGS=50
#   MAX_MINUTES=10
#   OUT_CSV=airbnb_results.csv
#   HEADLESS=1
#   PROXY=socks5://user:pass@host:port  (optionnel)
#
# CLI:
#   python scrape_airbnb.py                            # collecte via START_URL puis scrape
#   python scrape_airbnb.py <url1> <url2> ...          # scrape direct
#   (ou fournir urls.txt, une URL par ligne)

import asyncio, csv, json, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "50"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", "10"))
OUT_CSV = os.getenv("OUT_CSV", "airbnb_results.csv")
HEADLESS = os.getenv("HEADLESS", "1") != "0"
PROXY = os.getenv("PROXY") or None
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT_MS", "60000"))
INPUT_TXT = os.getenv("INPUT_TXT", "urls.txt")

HEADER = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]

LICENSE_PATTERNS = [
    r"(?:licen[cs]e|licen[cs]e\s*number|registration(?:\s*number)?|permit(?:\s*number)?)\s*[:\-]?\s*([A-Z0-9\-\/_. ]{4,})",
    r"(?:num[eé]ro\s*d['e]nregistrement|num[eé]ro\s*de\s*licence)\s*[:\-]?\s*([A-Z0-9\-\/_. ]{4,})",
    r"(?:DTCM|DED|RERA|Tourism|Dubai\s*Tourism)\s*[:\-]?\s*([A-Z0-9\-\/_.]{4,})",
]
JOINED_PATTERNS = [
    r"Joined\s+in\s+(\d{4})",
    r"Membre\s+depuis\s+(\d{4})",
    r"Se\s+uni[oó]\s+en\s+(\d{4})",
    r"Mitglied\s+seit\s+(\d{4})",
    r"加入于\s*(\d{4})",
]
RATING_PATTERNS = [
    r"(\d+(?:[.,]\d+)?)\s*(?:out of|sur)\s*5",
    r"([4-5](?:[.,]\d+)?)\s*[★⭐]",
]

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def uniq(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def write_header_if_needed(path: str):
    need = not Path(path).exists() or Path(path).stat().st_size == 0
    if need:
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(HEADER)

def append_row(path: str, row: dict):
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([row.get(k,"") for k in HEADER])

def find_by_regex(text: str, patterns: List[str]) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text or "", re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None

async def accept_gates(page):
    # cliques best-effort pour cookies / modals
    selectors = [
        'button:has-text("Accept")','button:has-text("I agree")','button:has-text("Got it")',
        'button:has-text("Tout accepter")','button:has-text("Accepter")','button[aria-label="Close"]',
        '[data-testid="dialog"] button:has-text("OK")'
    ]
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            if await loc.count() > 0:
                await loc.click(timeout=1000)
        except Exception:
            pass

async def collect_urls_from_search(page, start_url: str, target: int, max_minutes: int) -> List[str]:
    print(f"START {start_url}")
    await page.goto(start_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    await accept_gates(page)
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except PWTimeout:
        pass

    t0 = time.time()
    urls: List[str] = []
    seen_count = 0
    last_height = 0

    while len(urls) < target and (time.time() - t0) < max_minutes*60:
        # scroll
        try:
            await page.evaluate("window.scrollBy(0, Math.max(800, window.innerHeight*0.9));")
            await page.wait_for_timeout(500)
        except Exception:
            pass

        # extraire les liens /rooms/
        try:
            anchors = await page.eval_on_selector_all('a[href*="/rooms/"]', 'els => els.map(e => e.href)')
        except Exception:
            anchors = []
        # filtrer
        candidates = []
        for a in anchors:
            if not isinstance(a, str): continue
            if "/rooms/" in a and "/reviews" not in a and "/experiences/" not in a:
                # normaliser
                q = a.split("?")[0].rstrip("/")
                candidates.append(q)
        urls = uniq(urls + candidates)

        # charger plus si présent
        for sel in ['button:has-text("Show more")','button:has-text("Voir plus")','button[aria-label^="Show more results"]']:
            try:
                if await page.locator(sel).count() > 0:
                    await page.locator(sel).first.click(timeout=1000)
                    await page.wait_for_timeout(800)
            except Exception:
                pass

        # stopper si pas de nouveau contenu
        new_count = len(urls)
        doc_h = await page.evaluate("document.body.scrollHeight").catch(lambda _: last_height)
        if new_count == seen_count and doc_h == last_height:
            # petit shake
            try:
                await page.evaluate("window.scrollBy(0, -200);"); await page.wait_for_timeout(200)
                await page.evaluate("window.scrollBy(0,  800);"); await page.wait_for_timeout(400)
            except Exception:
                pass
        else:
            seen_count, last_height = new_count, doc_h

    urls = urls[:target]
    print(f"FOUND_URLS {len(urls)}")
    return urls

async def safe_text(page, selector: str) -> Optional[str]:
    try:
        el = page.locator(selector).first
        await el.wait_for(state="attached", timeout=3000)
        txt = await el.text_content()
        return (txt or "").strip() or None
    except Exception:
        return None

async def extract_json_ld(page) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    try:
        scripts = page.locator('script[type="application/ld+json"]')
        n = await scripts.count()
        title = rating = host = None
        for i in range(n):
            raw = await scripts.nth(i).text_content()
            if not raw: continue
            # tolérer multiples blobs
            for blob in re.findall(r"\{.*?\}(?=\s*\{|\s*$)", raw, flags=re.S):
                try:
                    data = json.loads(blob)
                except Exception:
                    continue
                if isinstance(data, dict):
                    if not title and isinstance(data.get("name"), str):
                        title = data["name"].strip()
                    if not rating:
                        agg = data.get("aggregateRating") or {}
                        val = agg.get("ratingValue")
                        if isinstance(val, (int, float, str)):
                            rating = str(val).strip()
                    if not host:
                        prov = data.get("provider") or data.get("author") or {}
                        name = prov.get("name") if isinstance(prov, dict) else None
                        if isinstance(name, str):
                            host = name.strip()
        return title, rating, host
    except Exception:
        return None, None, None

async def extract_listing(page, url: str) -> dict:
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    await accept_gates(page)
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except PWTimeout:
        pass

    title_json, rating_json, host_json = await extract_json_ld(page)
    title = title_json or (await safe_text(page, "h1")) or (await safe_text(page, "title"))

    host_name = host_json or await safe_text(page, '[data-testid="user-profile-name"]')
    host_profile_url = None
    try:
        link = page.locator('a[href*="/users/show/"]').first
        if await link.count() > 0:
            href = await link.get_attribute("href")
            if href:
                host_profile_url = href if href.startswith("http") else ("https://www.airbnb.com"+href)
    except Exception:
        pass

    html = await page.content()
    host_joined = find_by_regex(html, JOINED_PATTERNS)

    host_overall_rating = rating_json
    if not host_overall_rating:
        # aria-label styles
        try:
            labels = await page.locator('[aria-label*="sur 5"], [aria-label*="out of 5"]').all_attribute_values("aria-label")
            if labels:
                host_overall_rating = find_by_regex(" ".join([x for x in labels if x]), RATING_PATTERNS)
        except Exception:
            pass
        if not host_overall_rating:
            host_overall_rating = find_by_regex(html, RATING_PATTERNS)

    # Licence / enregistrement
    license_code = None
    try:
        blocks = await page.locator("section, div, li").all_inner_texts()
        license_code = find_by_regex("\n".join(blocks), LICENSE_PATTERNS)
    except Exception:
        pass
    if not license_code:
        license_code = find_by_regex(html, LICENSE_PATTERNS)

    return {
        "url": url,
        "title": (title or "").strip(),
        "license_code": (license_code or "").strip(),
        "host_name": (host_name or "").strip(),
        "host_overall_rating": ((host_overall_rating or "").replace(",", ".")).strip(),
        "host_profile_url": (host_profile_url or "").strip(),
        "host_joined": (host_joined or "").strip(),
        "scraped_at": now_iso(),
    }

def read_urls_from_txt(path: str) -> List[str]:
    if not Path(path).exists():
        return []
    urls = [ln.strip() for ln in Path(path).read_text(encoding="utf-8").splitlines()
            if ln.strip().startswith("http")]
    return urls

async def main():
    # préparer contexte navigateur
    launch_args = ["--no-sandbox"]
    proxy = {"server": PROXY} if PROXY else None
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS, args=launch_args, proxy=proxy)
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120 Safari/537.36"),
            locale="fr-FR",
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        # déterminer les URLs à scraper
        cli_urls = [u for u in sys.argv[1:] if u.startswith("http")]
        file_urls = read_urls_from_txt(INPUT_TXT)
        urls: List[str] = uniq(cli_urls + file_urls)

        if not urls:
            try:
                urls = await collect_urls_from_search(page, START_URL, MAX_LISTINGS, MAX_MINUTES)
            except Exception as e:
                print(f"Failed to collect URLs: {e}")
                urls = []

        print(f"FOUND_URLS {len(urls)}")

        write_header_if_needed(OUT_CSV)
        count = 0
        for i, url in enumerate(urls, 1):
            try:
                print(f"[{i}/{len(urls)}] {url}")
                row = await extract_listing(page, url)
                append_row(OUT_CSV, row)
                count += 1
            except Exception as e:
                print(f"ERROR {url}: {e}")

        await context.close()
        await browser.close()

    print(f"SAVED {count} rows to {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
