from playwright.sync_api import sync_playwright
import csv, re, time, random, sys

# =========================
# COLLE ICI TON URL Airbnb de recherche (avec tes filtres)
SEARCH_URL = "https://www.airbnb.fr/s/Duba%C3%AF-centre~ville/homes?refinement_paths%5B%5D=%2Fhomes&acp_id=ed0ceecb-417e-4db7-a51a-b28705c30d67&date_picker_type=calendar&source=structured_search_input_header&search_type=unknown&flexible_trip_lengths%5B%5D=one_week&price_filter_input_type=2&price_filter_num_nights=9&channel=EXPLORE&place_id=ChIJg_kMcC9oXz4RBLnAdrBYzLU&query=Duba%C3%AF%20centre-ville&search_mode=regular_search"

OUTPUT = "airbnb_listings.csv"
MAX_LISTINGS = 1000        # limite maxi
MAX_SCROLL_ROUNDS = 300    # garde-fou
STALL_ROUNDS_LIMIT = 5     # stop si aucun nouveau lien pendant 5 tours
# =========================

RE_LICENSE_PRIMARY = re.compile(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", re.IGNORECASE)
RE_LICENSE_FALLBACK = re.compile(
    r"(?:Registration(?:\s*No\.|\s*Number)?|Permit|License|Licence|DTCM)[^\n\r]*?([A-Z0-9][A-Z0-9\-\/]{3,40})",
    re.IGNORECASE,
)
RE_HOST_RATING = re.compile(r"([0-5]\.\d{1,2})\s*(?:out of 5|·|/5|rating|reviews)", re.IGNORECASE)

def pause(a=0.9, b=1.8):
    time.sleep(random.uniform(a, b))

def clean(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

def extract_license(page) -> str:
    # 1) bloc officiel
    try:
        c = page.query_selector("div[data-testid='listing-permit-license-number']")
        if c:
            spans = c.query_selector_all("span")
            if spans and len(spans) >= 2:
                val = clean(spans[-1].inner_text())
                m = RE_LICENSE_PRIMARY.search(val) or RE_LICENSE_FALLBACK.search(val)
                if m:
                    return m.group(1).upper()
                if val:
                    return val.upper()
    except:
        pass
    # 2) fallback zones pertinentes
    for sel in [
        "div:has-text('Permit number')",
        "div:has-text('Dubai Tourism permit number')",
        "div:has-text('Registration')",
        "div:has-text('License')",
        "div:has-text('Licence')",
        "div:has-text('DTCM')",
        "section[aria-labelledby*='About this space']",
        "div[data-section-id='DESCRIPTION_DEFAULT']",
    ]:
        try:
            el = page.query_selector(sel)
            if el:
                txt = clean(el.inner_text())
                m = RE_LICENSE_PRIMARY.search(txt) or RE_LICENSE_FALLBACK.search(txt)
                if m:
                    return m.group(1).upper()
        except:
            continue
    # 3) tout le body
    try:
        body = clean(page.inner_text("body"))
        m = RE_LICENSE_PRIMARY.search(body) or RE_LICENSE_FALLBACK.search(body)
        if m:
            return m.group(1).upper()
    except:
        pass
    return ""

def collect_search_urls(page):
    """Scroll infini jusqu'à stabilisation du nombre d'URL."""
    urls, seen = [], set()
    stall_rounds = 0

    # attendre que les premiers résultats s’affichent
    try:
        page.wait_for_selector("a[href*='/rooms/']", timeout=30000)
    except:
        pass

    for i in range(MAX_SCROLL_ROUNDS):
        anchors = page.query_selector_all("a[href*='/rooms/']")
        new_count = 0
        for a in anchors:
            href = (a.get_attribute("href") or "").strip()
            if not href:
                continue
            if href.startswith("/"):
                href = "https://www.airbnb.com" + href
            href = href.split("?")[0]
            if "/rooms/" in href and href not in seen:
                seen.add(href)
                urls.append(href)
                new_count += 1

        print(f"Scroll {i+1:03d} — total URLs: {len(urls)} (+{new_count})")
        if len(urls) >= MAX_LISTINGS:
            break

        if new_count == 0:
            stall_rounds += 1
        else:
            stall_rounds = 0

        if stall_rounds >= STALL_ROUNDS_LIMIT:
            print("Aucun nouveau lien depuis plusieurs scrolls — fin du chargement.")
            break

        page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        pause(0.8, 1.3)

    return urls[:MAX_LISTINGS]

def scrape_host_profile(context, host_url):
    """Récupère note globale, nb d'annonces et date d'inscription en scrollant la page profil."""
    note, nb_listings, joined = "", "", ""
    p = context.new_page()
    p.goto(host_url, timeout=90000)
    pause(0.8, 1.3)

    # Scroll la page profil pour forcer le chargement
    total_scrolls = 12
    for _ in range(total_scrolls):
        p.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        pause(0.3, 0.6)

    # Note globale (regex sur tout le body)
    try:
        body = p.inner_text("body")
        m = RE_HOST_RATING.search(body)
        if m:
            note = m.group(1)
    except:
        pass

    # Nombre d'annonces (compte des liens /rooms/ uniques)
    try:
        cards = p.query_selector_all("a[href*='/rooms/']")
        nb_listings = str(len({(c.get_attribute('href') or '').split('?')[0] for c in cards}))
    except:
        pass

    # Date d’inscription
    try:
        j = p.query_selector("span:has-text('Joined')") or p.query_selector("div:has-text('Joined')")
        if j:
            joined = clean(j.inner_text())
    except:
        pass

    p.close()
    return note, nb_listings, joined

def main():
    if not SEARCH_URL or "airbnb." not in SEARCH_URL:
        raise SystemExit("ERREUR: mets ton URL Airbnb dans SEARCH_URL.")

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        # on bloque les images pour aller plus vite et réduire le blocage
        context = pw.chromium.launch(headless=True).new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36",
            locale="en-US",
        )
        context.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())
        page = context.new_page()

        print("Ouverture recherche:", SEARCH_URL)
        page.goto(SEARCH_URL, timeout=120000)
        pause(2, 3)

        # 1) Collecte exhaustive des URLs (scroll infini)
        urls = collect_search_urls(page)
        print("Total URLs à visiter:", len(urls))

        # 2) Visite des annonces
        rows = []
        for idx, url in enumerate(urls, 1):
            try:
                page.goto(url, timeout=120000)
                pause(0.9, 1.4)

                # cookies éventuels
                try:
                    btn = page.query_selector("button:has-text('Accept')") or page.query_selector("button:has-text('OK')")
                    if btn: btn.click()
                except: pass

                # titre
                titre = ""
                try:
                    h1 = page.query_selector("h1")
                    titre = clean(h1.inner_text() if h1 else "")
                except: pass

                # code licence (précis)
                code_licence = extract_license(page)

                # hôte (nom + URL profil)
                url_profil, nom_hote = "", ""
                try:
                    host_link = page.query_selector("a[href*='/users/show/']")
                    if host_link:
                        hp = (host_link.get_attribute("href") or "")
                        if hp.startswith("/"): hp = "https://www.airbnb.com" + hp
                        url_profil = hp
                        txt = clean(host_link.inner_text() or "")
                        if txt: nom_hote = txt
                except: pass

                # profil hôte (note globale + nb annonces + joined)
                note_globale, nb_annonces, joined = "", "", ""
                if url_profil:
                    note_globale, nb_annonces, joined = scrape_host_profile(context, url_profil)

                rows.append({
                    "url_annonce": url,
                    "titre_annonce": titre,
                    "code_licence": code_licence,
                    "nom_hote": nom_hote,
                    "url_profil_hote": url_profil,
                    "note_globale_hote": note_globale,
                    "nb_annonces_hote": nb_annonces,
                    "date_inscription_hote": joined,
                })
                print(f"[{idx}/{len(urls)}] OK — {titre} — licence:{code_licence} — note_hote:{note_globale}")
            except Exception as e:
                print(f"[{idx}] ERREUR {url}: {e}")

        # 3) CSV (UTF-8 BOM)
        if rows:
            keys = list(rows[0].keys())
            with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for r in rows: w.writerow(r)
            print("CSV écrit:", OUTPUT)
        else:
            print("Aucune donnée collectée.")

        context.close()
        browser.close()

if __name__ == "__main__":
    main()
