# scrape_airbnb.py
# -*- coding: utf-8 -*-
import asyncio, csv, os, re, time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "100"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", "15"))
PROXY = os.getenv("PROXY") or None
OUT_CSV = os.getenv("OUT_CSV", "airbnb_results.csv")

# ---------- Utilities ----------

def ts():
    return datetime.now(timezone.utc).isoformat()

def uniq(seq):
    seen=set()
    out=[]
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

HOST_KEYWORDS = re.compile(
    r"(meet your host|rencontrez votre h[oô]te|conoce a tu anfitri[oó]n|dein gastgeber|il tuo host|seu anfitri[aã]o|conheça seu anfitri[aã]o|host|h[oô]te)",
    re.I
)

RATING_NEAR_HOST = re.compile(
    r"([0-5](?:[,.]\d{1,2})?)\s*(?:/|sur|de|von|da)?\s*5", re.I
)

REG_LABEL = re.compile(
    r"(registration|enregistr|licen[cs]e|permit|dtcm|الترخيص|licencia|licença|许可|등록)", re.I
)

REG_CODE = re.compile(
    r"\b([A-Z0-9]{3,}(?:-[A-Z0-9]{2,})+|\d{4,12})\b", re.I
)

JOINED_PAT = re.compile(
    r"(joined in|membre depuis|inscrit en|registrad[oa] en|iscritto dal|entrou em|登録|加入|mitglied seit)",
    re.I
)

def absolutize(base, href):
    try:
        return urljoin(base, href)
    except Exception:
        return href

async def maybe_click(page, locators, timeout=3000):
    for sel in locators:
        try:
            await page.locator(sel).first.click(timeout=timeout)
            return True
        except Exception:
            continue
    return False

async def dismiss_overlays(page):
    # Cookie/consent common buttons in many languages
    btn_texts = [
        "Accept all", "Accept", "OK", "Agree", "Got it",
        "Tout accepter", "Autoriser", "J'accepte", "D'accord",
        "Aceptar", "Permitir", "Aceitar", "Accetta", "Zustimmen"
    ]
    sels = [f'button:has-text("{t}")' for t in btn_texts] + [
        '[data-testid="accept-btn"]', '[aria-label*="accept"]'
    ]
    await maybe_click(page, sels)

async def wait_idle(page):
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except PWTimeout:
        pass

# ---------- Extraction on listing page ----------

async def extract_license_code(page):
    # Try to open "About this place" panel if present
    openers = [
        'button:has-text("About this place")',
        'button:has-text("À propos de ce logement")',
        'text=/^About this place$/',
        'text=/^À propos de ce logement$/',
        '[data-testid="pdp-about-this-home"]',
        '[data-testid="pdp-section-about-listing"]',
        'button:has-text("Afficher plus") >> nth=0',
        'button:has-text("Show more") >> nth=0',
    ]
    await maybe_click(page, openers, timeout=2000)
    # Give modal a moment if it opened
    await asyncio.sleep(0.5)

    # Search near registration labels
    code = None
    try:
        code = await page.evaluate(
            """(REG_LABEL_SRC, REG_CODE_SRC) => {
                const REG_LABEL = new RegExp(REG_LABEL_SRC, 'i');
                const REG_CODE = new RegExp(REG_CODE_SRC, 'i');

                function text(el){ return (el?.innerText||'').trim(); }

                const all = Array.from(document.querySelectorAll('div,section,li,p,span'));
                // 1) direct "label: code" in same node
                for(const el of all){
                    const t = text(el);
                    if(!t) continue;
                    if(REG_LABEL.test(t)){
                        const m = t.match(REG_CODE);
                        if(m) return m[1].trim();
                        // 2) otherwise look in next siblings
                        let nxt = el.nextElementSibling;
                        let hops = 0;
                        while(nxt && hops < 4){
                            const tt = text(nxt);
                            if(tt){
                                const m2 = tt.match(REG_CODE);
                                if(m2) return m2[1].trim();
                            }
                            nxt = nxt.nextElementSibling; hops++;
                        }
                        // 3) or look down inside el
                        for(const sub of el.querySelectorAll('*')){
                            const ts = text(sub);
                            if(!ts) continue;
                            const m3 = ts.match(REG_CODE);
                            if(m3) return m3[1].trim();
                        }
                    }
                }
                // 4) window text fallback within 120 chars after label word
                const body = document.body.innerText.replace(/\\s+/g,' ');
                const lab = body.search(REG_LABEL);
                if(lab >= 0){
                    const snippet = body.slice(lab, lab+200);
                    const m = snippet.match(REG_CODE);
                    if(m) return m[1].trim();
                }
                return null;
            }""",
            REG_LABEL.pattern, REG_CODE.pattern
        )
    except Exception:
        code = None
    return code

async def extract_host_block(page):
    # Find host anchor inside the "Meet your host" area
    result = {"host_profile_url": None, "host_name": None, "host_overall_rating": None}

    try:
        data = await page.evaluate(
            """(HOST_KEY_RE, RATING_RE) => {
                const HOST_RE = new RegExp(HOST_KEY_RE, 'i');
                const RATING_RE = new RegExp(RATING_RE, 'i');

                function clean(t){ return (t||'').replace(/\\s+/g,' ').trim(); }

                const anchors = Array.from(document.querySelectorAll('a[href*="/users/show/"]'));
                let hostA = null;

                // Prefer anchors whose ancestors talk about the host
                outer: for(const a of anchors){
                    let el = a, steps = 0;
                    while(el && steps < 6){
                        const txt = clean(el.innerText);
                        if(HOST_RE.test(txt)) { hostA = a; break outer; }
                        el = el.parentElement; steps++;
                    }
                }
                // Fallback: any anchor with aria-label containing host-like words
                if(!hostA){
                    for(const a of anchors){
                        const al = (a.getAttribute('aria-label')||'') + ' ' + (a.title||'');
                        if(HOST_RE.test(al)) { hostA = a; break; }
                    }
                }
                // Last fallback: first /users/show/ anchor on page
                if(!hostA) hostA = anchors[0] || null;

                let host_url = hostA ? new URL(hostA.getAttribute('href'), location.origin).toString() : null;

                // Host name: from nearby "Hosted by <name>" or text around anchor
                let host_name = null;
                if(hostA){
                    let box = hostA;
                    for(let i=0;i<6 && box;i++){ box = box.parentElement; }
                    const root = box || document.body;
                    const txt = clean(root.innerText);
                    // Try patterns in several languages
                    let m = txt.match(/Hosted by\\s+([^\\n•|,]+)/i) 
                         || txt.match(/H[ôo]te[ :]*\\s*([^\\n•|,]+)/i)
                         || txt.match(/Anfitri[óo]n[a]?[ :]*\\s*([^\\n•|,]+)/i)
                         || txt.match(/Gastgeber[ :]*\\s*([^\\n•|,]+)/i)
                         || txt.match(/Ospite[ :]*\\s*([^\\n•|,]+)/i);
                    if(m) host_name = clean(m[1]);
                    if(!host_name){
                        // take visible text inside the anchor if it looks like a name
                        const t = clean(hostA.innerText);
                        if(t && t.length <= 60) host_name = t;
                    }
                }

                // Rating: search within the same host section for "x.xx / 5"
                let rating = null;
                if(hostA){
                    let sec = hostA;
                    for(let i=0;i<6 && sec;i++){ sec = sec.parentElement; }
                    const nodes = Array.from((sec||document.body).querySelectorAll('[aria-label],[role],span,div'));
                    // aria-label like "4.9 out of 5"
                    for(const n of nodes){
                        const al = (n.getAttribute('aria-label')||'');
                        const m = al.match(RATING_RE);
                        if(m){ rating = m[1].replace(',', '.'); break; }
                    }
                    if(!rating){
                        const txt = (sec||document.body).innerText;
                        const m = txt.match(RATING_RE);
                        if(m) rating = m[1].replace(',', '.');
                    }
                }

                return {host_url, host_name, rating};
            }""",
            HOST_KEYWORDS.pattern, RATING_NEAR_HOST.pattern
        )
    except Exception:
        data = None

    if data:
        result["host_profile_url"] = data.get("host_url")
        result["host_name"] = data.get("host_name")
        result["host_overall_rating"] = data.get("rating")

    # Normalize
    if result["host_overall_rating"]:
        try:
            result["host_overall_rating"] = str(round(float(result["host_overall_rating"].replace(',', '.')), 2))
        except Exception:
            result["host_overall_rating"] = None
    return result

async def extract_from_host_profile(context, host_url, current_host_name, current_rating):
    out = {"host_name": current_host_name, "host_overall_rating": current_rating, "host_joined": None}
    if not host_url: return out
    p = await context.new_page()
    try:
        await p.goto(host_url, timeout=35000); await wait_idle(p); await dismiss_overlays(p)
        # Joined text
        try:
            joined = await p.evaluate(
                """(JOINED_PAT_SRC) => {
                    const JP = new RegExp(JOINED_PAT_SRC, 'i');
                    const txt = (document.body.innerText||'').replace(/\\s+/g,' ');
                    const idx = txt.search(JP);
                    if(idx>=0){
                        // take up to 40 chars after the match
                        const snip = txt.slice(idx, idx+80);
                        // keep "Month YYYY" or just "YYYY"
                        const m = snip.match(/(Jan\\w+|F[ée]v\\w+|Mar\\w+|Avr\\w+|Mai|Jun\\w+|Jul\\w+|Ao[ûu]t|Sep\\w+|Oct\\w+|Nov\\w+|D[ée]c\\w+|January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}|\\b\\d{4}\\b/);
                        if(m) return m[0];
                    }
                    return null;
                }""",
                JOINED_PAT.pattern
            )
        except Exception:
            joined = None
        if joined: out["host_joined"] = joined

        # Host name fallback from profile header
        if not out["host_name"]:
            try:
                nm = await p.locator('h1, h2').first.inner_text(timeout=2000)
                out["host_name"] = nm.strip()
            except Exception:
                pass

        # Rating fallback from aria-label on profile
        if not out["host_overall_rating"]:
            try:
                al_nodes = await p.locator('[aria-label*="out of 5"], [aria-label*="sur 5"], [aria-label*="de 5"], [aria-label*="von 5"]').all()
                for n in al_nodes:
                    al = (await n.get_attribute('aria-label')) or ''
                    m = re.search(r'([0-5](?:[.,]\d{1,2})?)', al)
                    if m:
                        out["host_overall_rating"] = m.group(1).replace(',', '.')
                        break
            except Exception:
                pass
    finally:
        await p.close()
    return out

async def get_listing_urls(page, max_urls):
    await page.goto(START_URL, timeout=45000)
    await wait_idle(page); await dismiss_overlays(page)

    urls=set()
    t0=time.time()
    last_height=0
    while len(urls)<max_urls and (time.time()-t0) < MAX_MINUTES*60:
        # collect
        hrefs = await page.eval_on_selector_all(
            'a[href^="/rooms/"], a[href*="/rooms/"]',
            "els => els.map(e => e.getAttribute('href'))"
        )
        for h in hrefs:
            if not h: continue
            if "/rooms/" not in h: continue
            # Strip query to stabilize
            u = urljoin(page.url, h.split('?')[0])
            urls.add(u)
            if len(urls)>=max_urls: break
        # scroll
        await page.mouse.wheel(0, 2000)
        await asyncio.sleep(0.6)
        # break if page end
        height = await page.evaluate("document.body.scrollHeight")
        if height == last_height: break
        last_height = height

    return uniq(list(urls))[:max_urls]

# ---------- Main ----------

async def run():
    print(f"START {START_URL}")
    browser_args = ["--disable-blink-features=AutomationControlled"]
    ctx_kwargs = {
        "locale": "fr-FR",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "accept_downloads": False,
        "java_script_enabled": True,
        "bypass_csp": True,
        "viewport": {"width": 1280, "height": 900},
        "extra_http_headers": {"Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"},
    }
    if PROXY:
        ctx_kwargs["proxy"] = {"server": PROXY}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=browser_args)
        context = await browser.new_context(**ctx_kwargs)
        page = await context.new_page()

        rows=[]
        try:
            urls = await get_listing_urls(page, MAX_LISTINGS)
            print(f"FOUND_URLS {len(urls)}")
            for i,u in enumerate(urls,1):
                try:
                    await page.goto(u, timeout=45000); await wait_idle(page); await dismiss_overlays(page)
                    # Title
                    try:
                        title = (await page.locator('h1').first.inner_text(timeout=3000)).strip()
                    except Exception:
                        try:
                            title = (await page.title()).strip()
                        except Exception:
                            title = ""

                    # Host block
                    host_blk = await extract_host_block(page)
                    host_url = host_blk.get("host_profile_url")
                    host_name = host_blk.get("host_name")
                    host_rating = host_blk.get("host_overall_rating")

                    # License code
                    license_code = await extract_license_code(page)

                    # Fallback enrichment from host profile if needed
                    if host_url or (not host_rating) or (not host_name):
                        enrich = await extract_from_host_profile(context, host_url, host_name, host_rating)
                        host_name = enrich.get("host_name") or host_name
                        host_rating = enrich.get("host_overall_rating") or host_rating
                        host_joined = enrich.get("host_joined")
                    else:
                        host_joined = None

                    row = {
                        "url": u,
                        "title": title,
                        "license_code": license_code or "",
                        "host_name": host_name or "",
                        "host_overall_rating": host_rating or "",
                        "host_profile_url": host_url or "",
                        "host_joined": host_joined or "",
                        "scraped_at": ts(),
                    }
                    rows.append(row)
                    print(f"[{i}/{len(urls)}] {u} | host={row['host_name'] or '?'} | lic={row['license_code'] or '?'} | rating={row['host_overall_rating'] or '?'}")
                except Exception as e:
                    print(f"[{i}/{len(urls)}] {u} ERROR {e}")

        finally:
            await context.close()
            await browser.close()

        # Write CSV
        fieldnames = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in rows: w.writerow(r)
        print(f"SAVED {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(run())
