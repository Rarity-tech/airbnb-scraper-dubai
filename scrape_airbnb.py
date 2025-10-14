# scrape_airbnb.py
# Variables d'env:
#   START_URL=https://www.airbnb.com/s/Dubai/homes
#   MAX_LISTINGS=50
#   MAX_MINUTES=10  (non utilisé ici, garder si besoin)
#   PROXY=socks5://user:pass@host:port  (optionnel)
import asyncio, csv, os, re, json, html
from datetime import datetime
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright

from pathlib import Path

# —— Reprise exacte du parseur JSON embarqué (mêmes fonctions que ci-dessus) ——
SCRIPT_RE = re.compile(r"<script[^>]*>(?P<code>[\s\S]*?)</script>", re.I)
HOST_JOINED_RE = re.compile(r"(Joined in|A rejoint Airbnb en)\s+(\d{4})", re.I)
LICENSE_KEYS = {"license","licence","license_num","license_number","registration","permit","dtcm_permit","dtcmPermit"}
PROFILE_PATH_RE = re.compile(r"^/users/show/\d+/?$", re.I)

def _json_blobs(html_text: str):
    for m in SCRIPT_RE.finditer(html_text):
        code = html.unescape(m.group("code").strip())
        l = code.find("{"); r = code.rfind("}")
        if l >= 0 and r > l:
            payload = code[l:r+1]
            try: yield json.loads(payload)
            except Exception:
                try:
                    payload = payload.replace("\\x3c","<")
                    yield json.loads(payload)
                except Exception:
                    continue

def _walk(obj, path=""):
    if isinstance(obj, dict):
        for k,v in obj.items():
            yield from _walk(v, f"{path}.{k}" if path else k)
            yield (path, k, v)
    elif isinstance(obj, list):
        for i,v in enumerate(obj):
            yield from _walk(v, f"{path}[{i}]")

def extract_from_listing_html(html_text: str):
    out = {"url":"","title":"","license_code":"","host_name":"","host_overall_rating":"",
           "host_profile_url":"","host_joined":""}
    blobs = list(_json_blobs(html_text))
    for b in blobs:
        for _, k, v in _walk(b):
            if k in ("title","listingName","localized_title","p3_title") and isinstance(v,str) and not out["title"]:
                out["title"] = v.strip()
            if k in ("canonicalUrl","url") and isinstance(v,str) and "/rooms/" in v and not out["url"]:
                out["url"] = v.split("?")[0]
    if not out["title"]:
        m = re.search(r"<title>(.*?)</title>", html_text, re.I|re.S)
        if m: out["title"] = re.sub(r"\s+"," ",html.unescape(m.group(1))).strip()
    if not out["url"]:
        m = re.search(r'https?://www\.airbnb\.[a-z.]+/rooms/\d+', html_text, re.I)
        if m: out["url"] = m.group(0)

    host_obj = None
    for b in blobs:
        cands = []
        for path, k, v in _walk(b):
            if isinstance(v, dict):
                prof = v.get("profile_path") or v.get("profilePath") or v.get("public_profile_url")
                if isinstance(prof,str) and PROFILE_PATH_RE.match(prof):
                    cands.append(v)
            if k in ("primary_host","primaryHost") and isinstance(v,dict):
                cands.append(v)
        if cands:
            host_obj = cands[0]; break
    if host_obj:
        name = host_obj.get("name") or host_obj.get("first_name") or host_obj.get("display_name")
        if name: out["host_name"] = str(name).strip()
        prof = host_obj.get("profile_path") or host_obj.get("profilePath") or host_obj.get("public_profile_url")
        if isinstance(prof,str) and PROFILE_PATH_RE.match(prof):
            out["host_profile_url"] = "https://www.airbnb.com" + prof if prof.startswith("/") else prof
        joined = host_obj.get("joined_on") or host_obj.get("joinedOn") or host_obj.get("member_since")
        if isinstance(joined,str):
            m = re.search(r"\b(20\d{2}|19\d{2})\b", joined)
            if m: out["host_joined"] = m.group(1)

    lic = ""
    for b in blobs:
        for _, k, v in _walk(b):
            if k in LICENSE_KEYS and isinstance(v,str) and v.strip():
                lic = v.strip(); break
        if lic: break
    if not lic:
        txt = html_text
        for p in [
            r"(?:license|licence|registration|permit|dtcm|dubai tourism)\s*(?:number|no\.?)?\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
            r"(?:num[eé]ro\s+d['’]enregistrement)\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
            r"(?:num[eé]ro\s+de\s+licen[cs]e|enregistrement)\s*[:\-]?\s*([A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,})",
        ]:
            m = re.search(p, txt, re.I)
            if m: lic = m.group(1).strip(" .:-"); break
    out["license_code"] = lic
    return out

def extract_host_rating_from_profile(html_text: str) -> str:
    blobs = list(_json_blobs(html_text))
    for b in blobs:
        for _, k, v in _walk(b):
            if k in ("overall_rating","overallRating","host_overall_rating") and isinstance(v,(int,float,str)):
                try: return f"{float(v):.2f}".rstrip("0").rstrip(".")
                except Exception: pass
    m = re.search(r'(\d\.\d)\s*(?:out of 5)?\s*(?:overall rating|host rating|note de l[’\' ]h[ôo]te)', html_text, re.I)
    if m: return m.group(1)
    m = re.search(r'\b(\d\.\d)\b(?=[^\n]{0,40}(reviews|avis))', html_text, re.I)
    return m.group(1) if m else ""

# —— Scraper ——
START_URL    = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "50"))
PROXY        = os.getenv("PROXY", "").strip()
ROOM_URL_RE  = re.compile(r"/rooms/\d+")

async def collect_listing_urls(page, limit):
    seen, urls = set(), []
    while len(urls) < limit:
        anchors = await page.locator("a[href*='/rooms/']").all()
        for a in anchors:
            href = await a.get_attribute("href")
            if not href: continue
            if href.startswith("/"): href = urljoin("https://www.airbnb.com", href)
            try:
                path = urlparse(href).path or ""
            except Exception:
                continue
            m = ROOM_URL_RE.search(path)
            if not m: continue
            u = "https://www.airbnb.com" + m.group(0)
            if u not in seen:
                seen.add(u); urls.append(u)
                if len(urls) >= limit: break
        if len(urls) >= limit: break
        await page.mouse.wheel(0, 2400)
        await page.wait_for_timeout(900)
    return urls[:limit]

async def get_text(page):
    # renvoie HTML complet après stabilisation
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=30000)
    except Exception:
        pass
    await page.wait_for_timeout(800)  # laisse hydrater le JSON
    return await page.content()

async def run():
    pw = await async_playwright().start()
    launch_kwargs = dict(headless=True, args=["--disable-blink-features=AutomationControlled"])
    if PROXY: launch_kwargs["proxy"] = {"server": PROXY}
    browser = await pw.chromium.launch(**launch_kwargs)
    context = await browser.new_context(
        viewport={"width":1366,"height":2200},
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118 Safari/537.36",
        locale="en-US",
    )
    page = await context.new_page()
    await page.goto(START_URL, wait_until="domcontentloaded")
    urls = await collect_listing_urls(page, MAX_LISTINGS)
    print("FOUND_URLS", len(urls))

    rows = []
    for i,u in enumerate(urls,1):
        await page.goto(u, wait_until="domcontentloaded")
        listing_html = await get_text(page)
        row = extract_from_listing_html(listing_html)
        # profil hôte si dispo
        if row["host_profile_url"]:
            await page.goto(row["host_profile_url"], wait_until="domcontentloaded")
            prof_html = await get_text(page)
            row["host_overall_rating"] = extract_host_rating_from_profile(prof_html)
            # joined dans le profil si pas déjà trouvé
            if not row["host_joined"]:
                m = HOST_JOINED_RE.search(prof_html)
                if m: row["host_joined"] = m.group(2)
        row["scraped_at"] = datetime.utcnow().isoformat(timespec="seconds")
        rows.append(row)
        print(f"[{i}/{len(urls)}] {u} | host={row['host_name']} | lic={row['license_code']} | rating={row['host_overall_rating']}")

    await browser.close(); await pw.stop()

    with open("airbnb_results.csv","w",encoding="utf-8-sig",newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"
        ])
        w.writeheader(); w.writerows(rows)
    print(f"SAVED {len(rows)} rows to airbnb_results.csv")

if __name__ == "__main__":
    asyncio.run(run())
