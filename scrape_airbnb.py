# scrape_airbnb.py
# Python 3.11 + Playwright 1.55
import asyncio, csv, os, re, sys, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.getenv("MAX_LISTINGS", "50"))
MAX_MIN     = float(os.getenv("MAX_MINUTES", "10"))
PROXY       = os.getenv("PROXY", "").strip()

OUT_CSV = "airbnb_results.csv"

# ---- utils ------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def uniq(seq: List[str]) -> List[str]:
    s, out = set(), []
    for x in seq:
        if x and x not in s:
            s.add(x); out.append(x)
    return out

def absolutize(base: str, href: Optional[str]) -> Optional[str]:
    if not href:
        return None
    return urljoin(base, href)

def extract_rating(text: str) -> Optional[float]:
    t = text.replace(",", ".")
    nums = re.findall(r"(\d\.\d{1,2})", t)
    for s in nums:
        try:
            f = float(s)
            if 3.5 <= f <= 5.0:
                return round(f, 2)
        except:  # noqa
            pass
    return None

_LICENSE_MARKERS = [
    r"Infos? d['’]enregistrement",
    r"Information[s]?\s+d['’]enregistrement",
    r"Registration info",
    r"Registration information",
    r"License(?: number)?",
    r"Licence(?: number)?",
    r"许可|허가|ライセンス|رقم[ \u00A0]?الرخصة|التسجيل",
]

def _sanitize_line(s: str) -> str:
    s = s.strip()
    # only keep reasonable characters for codes
    s = re.sub(r"[^\w\s\-\/\.]", "", s, flags=re.U)
    return s.strip()

def extract_license_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    # try: marker then the next non-empty line
    for m in _LICENSE_MARKERS:
        p = re.compile(m + r"\s*[\r\n]+([^\r\n]+)", re.I | re.U)
        k = p.search(text)
        if k:
            line = _sanitize_line(k.group(1))
            if len(line) >= 3:
                return line
    # fallback: look for typical Dubai patterns anywhere in text
    t = text.replace("\u200f", " ")
    m = re.search(r"\b[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{3,}\b", t)
    if m:
        return m.group(0)
    m = re.search(r"\b\d{5,}\b", t)  # simple numeric like 1446477
    if m:
        return m.group(0)
    return None

HOST_HEADERS = [
    "Meet your host", "Meet your Host", "Hosted by", "Host details",
    "Rencontrez votre hôte", "Animé par", "Hôte :", "Profil de l’hôte",
    "Conoce a tu anfitrión", "Incontra il tuo host", "Lerne deinen Gastgeber kennen",
    "Conheça seu anfitrião", "Leer je host kennen", "Ev sahibinizle tanışın",
    "Познакомьтесь с хозяином", "تعرّف على مضيفك",
]

def build_host_section_selector() -> str:
    parts = [f"section:has-text('{t}')" for t in HOST_HEADERS]
    # add a generic fallback that still contains a user profile link
    parts.append("section:has(a[href*='/users/show/'])")
    return ", ".join(parts)

async def safe_text(loc) -> Optional[str]:
    try:
        t = await loc.inner_text()
        return (t or "").strip()
    except:
        return None

async def click_if_exists(page, selector: str, timeout: float = 2000) -> bool:
    try:
        btn = page.locator(selector)
        if await btn.first.is_visible(timeout=timeout):
            await btn.first.click()
            return True
    except:
        pass
    return False

# ---- scraping primitives -----------------------------------------------------

async def collect_listing_urls(page) -> List[str]:
    await page.goto(START_URL, wait_until="domcontentloaded")
    await page.wait_for_load_state("networkidle")

    # lazy load a bit
    for _ in range(8):
        await page.mouse.wheel(0, 2400)
        await page.wait_for_timeout(350)

    # grab anchors to /rooms/
    hrefs: List[str] = await page.locator("a[href*='/rooms/']").evaluate_all(
        "els => Array.from(new Set(els.map(e => e.href.split('#')[0].split('?')[0]).filter(u => u.includes('/rooms/'))))"
    )
    # prefer non-experiences
    hrefs = [u for u in hrefs if "/experiences/" not in u]
    return hrefs[:MAX_LIST]

async def get_host_joined(context, host_url: str) -> Optional[str]:
    if not host_url:
        return None
    p = await context.new_page()
    try:
        await p.goto(host_url, wait_until="domcontentloaded", timeout=15000)
        await p.wait_for_load_state("networkidle")
        # possible texts
        joined_txt = await safe_text(p.locator("text=/Joined in/i, text=/Membre depuis/i, text=/Inscrit en/i"))
        if joined_txt:
            # extract year or month-year
            m = re.search(r"(Joined in|Membre depuis|Inscrit en)\s+([A-Za-zÀ-ÿ]+)?\s*(\d{4})", joined_txt, re.I)
            if m:
                month = (m.group(2) or "").strip()
                year = m.group(3)
                return (month + " " + year).strip()
        # fallback: scan whole page
        body = await safe_text(p.locator("body"))
        if body:
            m = re.search(r"(Joined in|Membre depuis|Inscrit en)\s+([A-Za-zÀ-ÿ]+)?\s*(\d{4})", body, re.I)
            if m:
                month = (m.group(2) or "").strip()
                year = m.group(3)
                return (month + " " + year).strip()
        return None
    except:
        return None
    finally:
        await p.close()

async def extract_from_listing(context, url: str) -> Dict[str, Any]:
    page = await context.new_page()
    blobs: List[Any] = []

    async def on_response(resp):
        try:
            ct = resp.headers.get("content-type", "")
        except:
            ct = ""
        if "application/json" in ct and ("/api/" in resp.url or "graphql" in resp.url or "StaysPdp" in resp.url):
            try:
                j = await resp.json()
                blobs.append(j)
            except:
                pass

    page.on("response", on_response)

    title = None
    license_code = None
    host_name = None
    host_profile_url = None
    host_overall_rating: Optional[float] = None
    host_joined = None

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        # cookies / banners
        await click_if_exists(page, "button:has-text('Accept')") or await click_if_exists(page, "button:has-text('Accepter')")
        await page.wait_for_load_state("networkidle")

        # title
        try:
            title = await safe_text(page.locator("[data-testid='title'], h1").first)
        except:
            title = None

        # open description modal if present to expose "Infos d'enregistrement"
        opened = await click_if_exists(page, "button:has-text('Show more')") or await click_if_exists(page, "button:has-text('Afficher plus')")
        if opened:
            await page.wait_for_timeout(400)

        # scan visible containers for license code
        about_container = page.locator("div[role='dialog'], section:has(h2:has-text('About')), section:has(h2:has-text('A propos')), section:has(h2:has-text('À propos'))")
        txt = await safe_text(about_container) or await safe_text(page.locator("main"))
        license_code = extract_license_from_text(txt or "")

        # fallback: try JSON blobs
        if not license_code and blobs:
            try:
                dumped = str(blobs)
                license_code = extract_license_from_text(dumped)
            except:
                pass

        # close modal if any
        await click_if_exists(page, "div[role='dialog'] button[aria-label*='Close']") or await click_if_exists(page, "div[role='dialog'] button")

        # host section
        host_section_sel = build_host_section_selector()
        host_section = page.locator(host_section_sel).first

        if await host_section.is_visible(timeout=5000):
            sec_text = (await safe_text(host_section)) or ""
            # profile url
            try:
                href = await host_section.locator("a[href*='/users/show/']").first.get_attribute("href")
                host_profile_url = absolutize(url, href)
            except:
                host_profile_url = None

            # host name
            m = re.search(r"(Hosted by|Hôte\s*:|Animé par)\s+([^\n·|,]+)", sec_text, re.I)
            if m:
                host_name = m.group(2).strip()
            else:
                # try anchor text
                host_name = (await safe_text(host_section.locator("a[href*='/users/show/']").first)) or None
                if host_name:
                    host_name = re.sub(r"\s+", " ", host_name).strip()

            # rating
            host_overall_rating = extract_rating(sec_text)

        # if still missing rating, try generic rating element in the host card
        if host_overall_rating is None:
            try:
                rr = await safe_text(page.locator("section:has(a[href*='/users/show/']) span:has-text('·')").first)
                host_overall_rating = extract_rating(rr or "")
            except:
                pass

        # joined date (from host card if present, else profile)
        if host_section and await host_section.is_visible():
            sec_text = (await safe_text(host_section)) or ""
            m = re.search(r"(Joined in|Membre depuis|Inscrit en)\s+([A-Za-zÀ-ÿ]+)?\s*(\d{4})", sec_text, re.I)
            if m:
                month = (m.group(2) or "").strip()
                year = m.group(3)
                host_joined = (month + " " + year).strip()

        if not host_joined and host_profile_url:
            host_joined = await get_host_joined(context, host_profile_url)

    except PWTimeout:
        pass
    except Exception:
        pass
    finally:
        try:
            await page.close()
        except:
            pass

    return {
        "url": url,
        "title": (title or "").strip(),
        "license_code": license_code or "",
        "host_name": (host_name or "").strip(),
        "host_overall_rating": f"{host_overall_rating:.2f}" if isinstance(host_overall_rating, float) else "",
        "host_profile_url": host_profile_url or "",
        "host_joined": host_joined or "",
        "scraped_at": now_iso(),
    }

# ---- main -------------------------------------------------------------------

async def run() -> None:
    started = time.time()
    launch_opts = {
        "headless": True,
        "args": ["--disable-dev-shm-usage"],
    }
    if PROXY:
        launch_opts["proxy"] = {"server": PROXY}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_opts)
        context = await browser.new_context(viewport={"width": 1280, "height": 900})
        page = await context.new_page()

        print(f"START {START_URL}")
        try:
            urls = await collect_listing_urls(page)
        except Exception as e:
            print("Failed to collect URLs:", e)
            urls = []

        urls = uniq(urls)[:MAX_LIST]
        print(f"FOUND_URLS {len(urls)}")

        rows: List[Dict[str, Any]] = []
        for idx, u in enumerate(urls, 1):
            if (time.time() - started) / 60.0 > MAX_MIN:
                break
            rec = await extract_from_listing(context, u)
            rows.append(rec)
            print(f"[{idx}/{len(urls)}] {u} | host={rec['host_name']} | lic={rec['license_code']} | rating={rec['host_overall_rating']}")

        await context.close()
        await browser.close()

    # write CSV
    fieldnames = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"SAVED {len(rows)} rows to {OUT_CSV}")

def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        sys.exit(130)

if __name__ == "__main__":
    main()
