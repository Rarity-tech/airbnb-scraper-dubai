# scrape_airbnb.py
import asyncio, json, re, csv, os, sys, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.getenv("MAX_LISTINGS", "50"))
MAX_MIN     = int(os.getenv("MAX_MINUTES", "15"))
PROXY       = os.getenv("PROXY") or None

# ---------- utils ----------

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def uniq(seq):
    seen = set(); out=[]
    for s in seq:
        if s not in seen:
            seen.add(s); out.append(s)
    return out

def looks_like_license(s: str) -> bool:
    s = s.strip()
    if not s: return False
    # accept numeric-only Dubai codes, and alphanum with dashes like BUR-BUR-OFXPS
    return bool(re.search(r"(?:[A-Z0-9]{2,}[-\s]?){2,}[A-Z0-9]{2,}$", s, re.I)) \
        or bool(re.fullmatch(r"\d{4,}", s)) \
        or bool(re.search(r"(exempt|not required|n/?a)", s, re.I))

def clean_text(s: Optional[str]) -> str:
    if not s: return ""
    return re.sub(r"\s+", " ", s).strip()

def deep_find_values(data: Any, keys_regex: str) -> List[Tuple[str, Any]]:
    out=[]
    key_re = re.compile(keys_regex, re.I)
    def walk(x):
        if isinstance(x, dict):
            for k,v in x.items():
                if key_re.search(k):
                    out.append((k,v))
                walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)
    walk(data)
    return out

def first_float(s: str) -> Optional[float]:
    m = re.search(r"(\d+(?:[.,]\d+)?)", s)
    return float(m.group(1).replace(",", ".")) if m else None

# ---------- core extractors ----------

async def extract_via_dom(page) -> Dict[str, Optional[str]]:
    # Title
    title = await page.locator('[data-testid="title"], h1').first.text_content().catch(lambda _: None)
    title = clean_text(title)

    # host section root
    host_section = page.locator('section:has-text("Meet your host"), section:has-text("Rencontrez votre hôte"), [data-section-id*="HOST_PROFILE"]').first

    # host name
    host_name = await host_section.locator('a[href*="/users/show/"], a[aria-label^="Hosted by"], a:has(> div)').first.text_content().catch(lambda _: None)
    host_name = clean_text(host_name)

    # host profile url
    host_profile_url = await host_section.locator('a[href*="/users/show/"]').first.get_attribute("href").catch(lambda _: None)
    if host_profile_url and host_profile_url.startswith("/"):
        host_profile_url = "https://www.airbnb.com" + host_profile_url

    # host overall rating
    rating_text = await host_section.locator('[aria-label*="overall rating"], [aria-label*="Note globale"], [aria-label*="note"], span, div').all_text_contents().catch(lambda _: [])
    overall = None
    for t in rating_text:
        v = first_float(t)
        if v and 3.0 <= v <= 5.0:
            overall = f"{v:.2f}"
            break

    # host joined
    host_joined = None
    all_text = " ".join(await host_section.all_text_contents().catch(lambda _: []))
    m = re.search(r"(Host since|Hôte depuis)\s+([A-Za-zéû]+\.?\s*\d{4}|\d{4})", all_text, re.I)
    if m:
        host_joined = clean_text(m.group(2))

    # license code: scan visible text around “Infos d'enregistrement / Registration / License”
    lic = None
    # If an “About this space/À propos de ce logement” button exists, open it to reveal the block
    try:
        btn = page.locator('button:has-text("About this space"), button:has-text("À propos de ce logement"), a:has-text("À propos de ce logement"), a:has-text("About this space")').first
        if await btn.is_visible():
            await btn.click()
            # small wait for modal content
            await page.wait_for_timeout(400)
    except:
        pass

    text_nodes = await page.locator('div, p, li, span').all_text_contents().catch(lambda _: [])
    for i, t in enumerate(text_nodes):
        t_low = t.lower()
        if re.search(r"(registration|license|licen[cs]e|enregistrement|permit|perm[iy]t)", t_low):
            # look ahead a few nodes for the code line
            for j in range(i+1, min(i+6, len(text_nodes))):
                cand = clean_text(text_nodes[j])
                if looks_like_license(cand):
                    lic = cand
                    break
        if lic: break

    return {
        "title": title or "",
        "host_name": host_name or "",
        "host_profile_url": host_profile_url or "",
        "host_overall_rating": overall or "",
        "host_joined": host_joined or "",
        "license_code": lic or "",
    }

async def extract_listing(page, url: str) -> Dict[str, str]:
    # Collect GraphQL/JSON responses for robust parsing
    blobs: List[Dict[str, Any]] = []
    def on_response(resp):
        ct = resp.headers.get("content-type", "")
        if "application/json" in ct and ("StaysPdp" in resp.url or "/api/v3" in resp.url):
            try:
                blobs.append(resp.json())
            except:
                pass
    page.on("response", on_response)

    await page.goto(url, wait_until="domcontentloaded")
    # late content
    await page.wait_for_timeout(1200)

    # JSON-LD as another source
    jsonld_data=[]
    for el in await page.locator('script[type="application/ld+json"]').all():
        try:
            txt = await el.text_content()
            if txt:
                jsonld_data.append(json.loads(txt))
        except:
            pass

    # DOM fallback details
    dom = await extract_via_dom(page)

    # From GraphQL/JSON: license, host id, name, rating, joined
    lic = dom["license_code"]
    host_name = dom["host_name"]
    host_profile_url = dom["host_profile_url"]
    host_overall = dom["host_overall_rating"]
    host_joined = dom["host_joined"]

    # scan deep for license-like values
    for blob in blobs + jsonld_data:
        for k,v in deep_find_values(blob, r"license|licen[cs]e|registration|regulatory|cityRegistration"):
            if isinstance(v, str) and looks_like_license(v):
                lic = v.strip()
                break
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, str) and looks_like_license(item):
                        lic = item.strip(); break
        if lic: break

    # host id and profile
    host_id=None
    for blob in blobs:
        # search for primary host id
        for k,v in deep_find_values(blob, r"(primary_?host|hostProfile|host)"):
            if isinstance(v, dict):
                cand = v.get("id") or v.get("userId") or v.get("hostId")
                if isinstance(cand, (str,int)):
                    host_id = str(cand)
            if not host_name and isinstance(v, dict):
                host_name = v.get("firstName") or v.get("name") or host_name
            if not host_overall and isinstance(v, dict):
                rating = v.get("overallRating") or v.get("overall_rating")
                if rating:
                    try: host_overall = f"{float(rating):.2f}"
                    except: pass
            if not host_joined and isinstance(v, dict):
                # examples: memberSince, createdAt
                joined = v.get("memberSince") or v.get("createdAt")
                if isinstance(joined, str):
                    host_joined = joined[:10]
        if host_id: break

    if not host_profile_url and host_id:
        host_profile_url = f"https://www.airbnb.com/users/show/{host_id}"

    # As last resort, try JSON-LD host url/name
    if jsonld_data:
        def find_host(d):
            if isinstance(d, dict):
                if "host" in d and isinstance(d["host"], dict):
                    return d["host"]
                for v in d.values():
                    x = find_host(v)
                    if x: return x
            if isinstance(d, list):
                for v in d:
                    x = find_host(v)
                    if x: return x
            return None
        h=find_host(jsonld_data)
        if h:
            if not host_name:
                host_name = clean_text(h.get("name"))
            if not host_profile_url:
                u = h.get("url")
                if isinstance(u,str) and "/users/show/" in u:
                    host_profile_url = u

    return {
        "url": url,
        "title": dom["title"],
        "license_code": lic or "",
        "host_name": host_name or "",
        "host_overall_rating": host_overall or "",
        "host_profile_url": host_profile_url or "",
        "host_joined": host_joined or "",
        "scraped_at": now_iso(),
    }

async def collect_listing_urls(page) -> List[str]:
    await page.goto(START_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(1200)
    cards = page.locator('a[href*="/rooms/"]:not([href*="experiences"])')
    hrefs = uniq([u async for u in cards.evaluate_all("els => els.map(e=>e.href)")])
    # keep only rooms links
    out=[u.split("?")[0] for u in hrefs if "/rooms/" in u]
    return out[:MAX_LIST]

async def main():
    t0=time.time()
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context_args = dict(locale="fr-FR", user_agent="Mozilla/5.0 AirbnbScraper")
        if PROXY:
            context_args["proxy"]={"server":PROXY}
        context = await browser.new_context(**context_args,
            extra_http_headers={"Accept-Language":"fr, en;q=0.8"})
        page = await context.new_page()

        urls = await collect_listing_urls(page)
        print(f"START {START_URL}")
        print(f"FOUND_URLS {len(urls)}")

        rows=[]
        for i,u in enumerate(urls,1):
            if (time.time()-t0)/60.0 > MAX_MIN: break
            try:
                row = await extract_listing(page, u)
                print(f"[{i}/{len(urls)}] {u} | host={row['host_name'] or '?'} | lic={row['license_code'] or '?'} | rating={row['host_overall_rating'] or '?'}")
                rows.append(row)
            except Exception as e:
                print(f"[{i}/{len(urls)}] {u} | ERROR {e}")
        await context.close(); await browser.close()

    # write CSV
    fp = Path("airbnb_results.csv")
    with fp.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"])
        w.writeheader()
        for r in rows: w.writerow(r)
    print(f"SAVED {len(rows)} rows to {fp.name}")

if __name__ == "__main__":
    asyncio.run(main())
