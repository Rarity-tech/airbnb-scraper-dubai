# filename: scrape_airbnb_listings.py
# deps: pip install playwright && playwright install chromium
import asyncio, csv, json, os, re, sys, time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout

OUT_CSV = os.getenv("OUT_CSV", "airbnb_results.csv")
INPUT_TXT = os.getenv("INPUT_TXT", "urls.txt")
HEADLESS = os.getenv("HEADLESS", "1") != "0"
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT_MS", "45000"))

# --- helpers -----------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def csv_writer(path: str):
    first = not Path(path).exists()
    f = open(path, "a", newline="", encoding="utf-8")
    w = csv.writer(f)
    if first:
        w.writerow(["url","title","license_code","host_name",
                    "host_overall_rating","host_profile_url",
                    "host_joined","scraped_at"])
    return f, w

async def safe_text(page: Page, selector: str) -> Optional[str]:
    try:
        el = page.locator(selector).first
        await el.wait_for(state="attached", timeout=3000)
        txt = (await el.text_content()) or ""
        return txt.strip() or None
    except Exception:
        return None

async def click_if_visible(page: Page, selectors: List[str]):
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() > 0:
                await loc.first.click(timeout=1000)
        except Exception:
            pass

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

def find_by_regex(text: str, patterns: List[str]) -> Optional[str]:
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None

async def extract_json_ld(page: Page) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (title, rating, host_name) if present in JSON-LD."""
    try:
        scripts = page.locator('script[type="application/ld+json"]')
        n = await scripts.count()
        title = rating = host = None
        for i in range(n):
            raw = await scripts.nth(i).text_content()
            if not raw:
                continue
            # Some pages contain multiple JSON objects concatenated
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
                        name = prov.get("name")
                        if isinstance(name, str):
                            host = name.strip()
        return title, rating, host
    except Exception:
        return None, None, None

async def accept_gates(page: Page):
    # Consent / language / login gates best-effort
    await click_if_visible(page, [
        'button:has-text("Accept")',
        'button:has-text("I agree")',
        'button:has-text("Tout accepter")',
        'button:has-text("Accepter")',
        'button[aria-label="Close"]',
        'button:has-text("OK")',
        'button:has-text("Got it")',
    ])

async def extract_listing(page: Page, url: str) -> dict:
    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)
    await accept_gates(page)
    # Let dynamic parts settle a bit
    try:
        await page.wait_for_load_state("networkidle", timeout=8000)
    except PWTimeout:
        pass

    # Title
    title_json, rating_json, host_json = await extract_json_ld(page)
    title = title_json or (await safe_text(page, "h1")) or (await safe_text(page, "title"))

    # Host block
    host_name = host_json
    if not host_name:
        # Common host card selectors
        host_name = await safe_text(page, '[data-testid="user-profile-name"]') \
                    or await safe_text(page, 'a[href*="/users/show/"]') \
                    or await safe_text(page, 'a[aria-label^="H\u00f4te"], a[aria-label^="Host"]')

    # Host profile URL
    host_profile_url = None
    try:
        host_link = page.locator('a[href*="/users/show/"]').first
        if await host_link.count() > 0:
            host_profile_url = await host_link.get_attribute("href")
            if host_profile_url and host_profile_url.startswith("/"):
                host_profile_url = "https://www.airbnb.com" + host_profile_url
    except Exception:
        pass

    # Host joined year (best-effort from visible text)
    page_text = (await page.content()) or ""
    host_joined = find_by_regex(page_text, JOINED_PATTERNS)

    # Rating
    host_overall_rating = rating_json
    if not host_overall_rating:
        # aria-label like "4,87 sur 5"
        aria_labels = await page.locator('[aria-label*="sur 5"], [aria-label*="out of 5"]').all_attribute_values("aria-label")
        joined = " ".join([a for a in aria_labels if a]) if aria_labels else ""
        host_overall_rating = find_by_regex(joined, RATING_PATTERNS)
        if not host_overall_rating:
            host_overall_rating = find_by_regex(page_text, RATING_PATTERNS)

    # License / registration code
    # Look in obvious “Permit/License/Registration” sections first
    license_code = None
    try:
        # Airbnb often renders a key-value row; scan visible text blocks
        blocks = await page.locator("section, div, li").all_inner_texts()
        big_txt = "\n".join(blocks)
        license_code = find_by_regex(big_txt, LICENSE_PATTERNS)
    except Exception:
        pass
    if not license_code:
        license_code = find_by_regex(page_text, LICENSE_PATTERNS)

    return {
        "url": url,
        "title": (title or "").strip(),
        "license_code": (license_code or "").strip(),
        "host_name": (host_name or "").strip(),
        "host_overall_rating": (host_overall_rating or "").replace(",", ".").strip(),
        "host_profile_url": (host_profile_url or "").strip(),
        "host_joined": (host_joined or "").strip(),
        "scraped_at": now_iso(),
    }

# --- main --------------------------------------------------------------------

async def run(urls: List[str]):
    f, writer = csv_writer(OUT_CSV)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120 Safari/537.36"),
            locale="fr-FR",
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()
        for i, url in enumerate(urls, 1):
            try:
                print(f"[{i}/{len(urls)}] {url}")
                row = await extract_listing(page, url)
                writer.writerow([row[k] for k in ["url","title","license_code","host_name",
                                                  "host_overall_rating","host_profile_url",
                                                  "host_joined","scraped_at"]])
                f.flush()
            except Exception as e:
                print(f"ERROR {url}: {e}")
        await context.close()
        await browser.close()
    f.close()
    print(f"SAVED {len(urls)} rows to {OUT_CSV}")

def read_urls_from_txt(path: str) -> List[str]:
    if not Path(path).exists():
        print(f"Missing {path}. Create it with one URL per line.")
        return []
    urls = [ln.strip() for ln in Path(path).read_text(encoding="utf-8").splitlines()
            if ln.strip() and ln.strip().startswith("http")]
    return urls

if __name__ == "__main__":
    urls = sys.argv[1:] if len(sys.argv) > 1 else read_urls_from_txt(INPUT_TXT)
    if not urls:
        print("No URLs provided.")
        sys.exit(0)
    asyncio.run(run(urls))
