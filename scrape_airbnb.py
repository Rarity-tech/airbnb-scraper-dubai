from playwright.sync_api import sync_playwright
import csv, re, time, random, urllib.parse

# =========================
# COLLE ICI TON URL Airbnb (ta page de recherche avec filtres), SANS items_offset
SEARCH_URL_BASE = "https://www.airbnb.fr/s/Duba%C3%AF-centre~ville/homes?refinement_paths%5B%5D=%2Fhomes&acp_id=ed0ceecb-417e-4db7-a51a-b28705c30d67&date_picker_type=calendar&source=structured_search_input_header&search_type=unknown&flexible_trip_lengths%5B%5D=one_week&price_filter_input_type=2&price_filter_num_nights=9&channel=EXPLORE&place_id=ChIJg_kMcC9oXz4RBLnAdrBYzLU&query=Duba%C3%AF%20centre-ville&search_mode=regular_search"
OUTPUT = "airbnb_listings.csv"

ITEMS_PER_PAGE = 20       # Airbnb retourne ~20 annonces par page
MAX_LISTINGS = 1000       # objectif total
MAX_OFFSET = 2000         # garde-fou (offset max essayé)
SECTION_OFFSETS = [0,1,2,3,4,5,6]  # variantes utilisées par Airbnb
# =========================

RE_LICENSE_PRIMARY = re.compile(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", re.IGNORECASE)
RE_LICENSE_FALLBACK = re.compile(r"(?:Registration(?:\s*No\.|\s*Number)?|Permit|License|Licence|DTCM)[^\n\r]*?([A-Z0-9][A-Z0-9\-\/]{3,40})", re.IGNORECASE)
RE_HOST_RATING = re.compile(r"([0-5]\.\d{1,2})\s*(?:out of 5|·|/5|rating|reviews)", re.IGNORECASE)

def pause(a=0.8, b=1.6): time.sleep(random.uniform(a,b))
def clean(s): return (s or "").replace("\xa0"," ").strip()

def build_page_url(base, items_offset, section_offset):
    # Ajoute/écrase items_offset & section_offset dans l’URL
    parsed = urllib.parse.urlparse(base)
    q = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    q["items_offset"] = str(items_offset)
    q["section_offset"] = str(section_offset)
    new_q = urllib.parse.urlencode(q)
    return urllib.parse.urlunparse(parsed._replace(query=new_q))

def extract_license(page):
    try:
        c = page.query_selector("div[data-testid='listing-permit-license-number']")
        if c:
            spans = c.query_selector_all("span")
            if spans and len(spans)>=2:
                val = clean(spans[-1].inner_text())
                m = RE_LICENSE_PRIMARY.search(val) or RE_LICENSE_FALLBACK.search(val)
                return (m.group(1) if m else val).upper()
    except: pass
    for sel in [
        "div:has-text('Permit number')","div:has-text('Dubai Tourism permit number')",
        "div:has-text('Registration')","div:has-text('License')","div:has-text('Licence')",
        "div:has-text('DTCM')","section[aria-labelledby*='About this space']","div[data-section-id='DESCRIPTION_DEFAULT']",
    ]:
        try:
            el = page.query_selector(sel)
            if el:
                txt = clean(el.inner_text())
                m = RE_LICENSE_PRIMARY.search(txt) or RE_LICENSE_FALLBACK.search(txt)
                if m: return m.group(1).upper()
        except: pass
    try:
        body = clean(page.inner_text("body"))
        m = RE_LICENSE_PRIMARY.search(body) or RE_LICENSE_FALLBACK.search(body)
        if m: return m.group(1).upper()
    except: pass
    return ""

def scrape_host_profile(context, host_url):
    note, nb_listings, joined = "", "", ""
    p = context.new_page(); p.goto(host_url, timeout=90000); pause()
    # scroll pour charger
    for _ in range(12):
        p.evaluate("window.scrollBy(0, document.body.scrollHeight)"); pause(0.25,0.5)
    try:
        body = p.inner_text("body"); m = RE_HOST_RATING.search(body)
        if m: note = m.group(1)
    except: pass
    try:
        cards = p.query_selector_all("a[href*='/rooms/']")
        nb_listings = str(len({(c.get_attribute('href') or '').split('?')[0] for c in cards}))
    except: pass
    try:
        j = p.query_selector("span:has-text('Joined')") or p.query_selector("div:has-text('Joined')")
        if j: joined = clean(j.inner_text())
    except: pass
    p.close(); return note, nb_listings, joined

def collect_urls_from_page(page):
    urls = []
    try:
        page.wait_for_selector("a[href*='/rooms/']", timeout=30000)
        for a in page.query_selector_all("a[href*='/rooms/']"):
            href = (a.get_attribute("href") or "").strip()
            if not href: continue
            if href.startswith("/"): href = "https://www.airbnb.com"+href
            href = href.split("?")[0]
            if "/rooms/" in href: urls.append(href)
    except: pass
    # dédupliquer en gardant l’ordre
    seen, out = set(), []
    for u in urls:
        if u not in seen: seen.add(u); out.append(u)
    return out

def main():
    if "airbnb." not in SEARCH_URL_BASE:
        raise SystemExit("Mets ton URL Airbnb dans SEARCH_URL_BASE.")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36",
            locale="en-US",
        )
        # accélérer : bloquer images/fonts
        context.route("**/*", lambda r: r.abort() if r.request.resource_type in ["image","media","font"] else r.continue_())
        page = context.new_page()

        all_urls, seen = [], set()
        items_offset = 0
        while items_offset <= MAX_OFFSET and len(all_urls) < MAX_LISTINGS:
            got_any = False
            for so in SECTION_OFFSETS:
                url = build_page_url(SEARCH_URL_BASE, items_offset, so)
                print(f"Page offset={items_offset} section_offset={so}")
                try:
                    page.goto(url, timeout=120000); pause()
                    page_urls = collect_urls_from_page(page)
                    # filtrer nouveaux
                    new = [u for u in page_urls if u not in seen]
                    for u in new: seen.add(u)
                    if new:
                        all_urls.extend(new); got_any = True
                        print(f"  +{len(new)} URLs (total {len(all_urls)})")
                        break  # cette page a donné des résultats → passe au prochain offset
                except Exception as e:
                    print("  (skip) erreur:", e)
            if not got_any:
                print("  Aucune nouvelle URL sur cet offset → on avance quand même.")
            items_offset += ITEMS_PER_PAGE
            if len(all_urls) >= MAX_LISTINGS: break

        all_urls = all_urls[:MAX_LISTINGS]
        print("Total d’URLs à visiter:", len(all_urls))

        rows = []
        for i, url in enumerate(all_urls, 1):
            try:
                page.goto(url, timeout=120000); pause()
                # cookies
                try:
                    btn = page.query_selector("button:has-text('Accept')") or page.query_selector("button:has-text('OK')")
                    if btn: btn.click()
                except: pass
                # titre
                titre = ""
                try:
                    h1 = page.query_selector("h1"); titre = clean(h1.inner_text() if h1 else "")
                except: pass
                # licence
                code = extract_license(page)
                # hôte
                nom_hote, host_url = "", ""
                try:
                    hl = page.query_selector("a[href*='/users/show/']")
                    if hl:
                        host_url = hl.get_attribute("href") or ""
                        if host_url.startswith("/"): host_url = "https://www.airbnb.com"+host_url
                        nom_hote = clean(hl.inner_text() or "")
                except: pass
                # profil
                note, nb, joined = "", "", ""
                if host_url:
                    note, nb, joined = scrape_host_profile(context, host_url)

                rows.append({
                    "url_annonce": url,
                    "titre_annonce": titre,
                    "code_licence": code,
                    "nom_hote": nom_hote,
                    "url_profil_hote": host_url,
                    "note_globale_hote": note,
                    "nb_annonces_hote": nb,
                    "date_inscription_hote": joined,
                })
                print(f"[{i}/{len(all_urls)}] OK — {titre} — {code} — note:{note}")
            except Exception as e:
                print(f"[{i}] ERREUR {url}: {e}")

        # CSV
        if rows:
            keys = list(rows[0].keys())
            with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=keys); w.writeheader()
                for r in rows: w.writerow(r)
            print("CSV écrit:", OUTPUT)
        else:
            print("Aucune donnée collectée.")

        context.close(); browser.close()

if __name__ == "__main__":
    main()
