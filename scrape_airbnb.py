from playwright.sync_api import sync_playwright
import csv, re, time, random

# =========================
# CONFIG : COLLE ICI TON URL Airbnb filtrée (celle de ta recherche)
# Ex: URL de "Downtown Dubai" avec tes filtres copiée depuis le navigateur.
SEARCH_URL = "https://www.airbnb.fr/s/Duba%C3%AF-centre~ville/homes?refinement_paths%5B%5D=%2Fhomes&acp_id=ed0ceecb-417e-4db7-a51a-b28705c30d67&date_picker_type=calendar&source=structured_search_input_header&search_type=unknown&flexible_trip_lengths%5B%5D=one_week&price_filter_input_type=2&price_filter_num_nights=9&channel=EXPLORE&place_id=ChIJg_kMcC9oXz4RBLnAdrBYzLU&query=Duba%C3%AF%20centre-ville&search_mode=regular_search"

MAX_LISTINGS = 1000   # nombre max d'annonces à collecter
SCROLLS = 100         # nombre de scrolls (augmente si tu veux charger plus)
OUTPUT = "airbnb_listings.csv"
# =========================

# Pattern licence DTCM typique (ex: BUR-GRA-T60A0) + fallback
RE_LICENSE_PRIMARY = re.compile(r"\b([A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,6})\b", re.IGNORECASE)
RE_LICENSE_FALLBACK = re.compile(
    r"(?:Registration(?:\s*No\.|\s*Number)?|Permit|License|Licence|DTCM)[^\n\r]*?([A-Z0-9][A-Z0-9\-\/]{3,40})",
    re.IGNORECASE,
)

def pause(a=0.9, b=2.0):
    time.sleep(random.uniform(a, b))

def clean_text(s: str) -> str:
    return (s or "").replace("\xa0", " ").strip()

def extract_license_precise(page) -> str:
    """
    1) Cherche le bloc officiel Airbnb:
       <div data-testid="listing-permit-license-number"><span>...</span><span>BUR-GRA-T60A0</span></div>
    2) Fallback: cherche dans sections voisines ou tout le body.
    """
    # Sélecteur officiel
    try:
        container = page.query_selector("div[data-testid='listing-permit-license-number']")
        if container:
            spans = container.query_selector_all("span")
            if spans and len(spans) >= 2:
                val = clean_text(spans[-1].inner_text())
                m = RE_LICENSE_PRIMARY.search(val) or RE_LICENSE_FALLBACK.search(val)
                if m:
                    return m.group(1).upper()
                if val:
                    return val.upper()
    except:
        pass

    # Fallback 1: zones textuelles pertinentes
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
                txt = clean_text(el.inner_text())
                m = RE_LICENSE_PRIMARY.search(txt) or RE_LICENSE_FALLBACK.search(txt)
                if m:
                    return m.group(1).upper()
        except:
            continue

    # Fallback 2: tout le body
    try:
        body_txt = clean_text(page.inner_text("body"))
        m = RE_LICENSE_PRIMARY.search(body_txt) or RE_LICENSE_FALLBACK.search(body_txt)
        if m:
            return m.group(1).upper()
    except:
        pass

    return ""

def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36",
            locale="en-US",
        )
        page = context.new_page()

        if not SEARCH_URL or "airbnb." not in SEARCH_URL:
            raise SystemExit("ERREUR: Mets ton URL Airbnb de recherche dans SEARCH_URL.")

        print("Ouverture recherche:", SEARCH_URL)
        page.goto(SEARCH_URL, timeout=120000)
        pause(2,4)

        # ------- collecte des URLs d’annonces (scroll infini = “pages suivantes”) -------
        urls, seen = [], set()
        for i in range(SCROLLS):
            anchors = page.query_selector_all("a[href*='/rooms/']")
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
            print(f"Scroll {i+1}/{SCROLLS} — {len(urls)} URLs")
            if len(urls) >= MAX_LISTINGS:
                break
            page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
            pause(1.0, 1.6)

        urls = urls[:MAX_LISTINGS]
        print("Total URLs à visiter:", len(urls))

        # ------- visite de chaque annonce -------
        rows = []
        for idx, url in enumerate(urls, 1):
            try:
                page.goto(url, timeout=120000)
                pause(1.1, 1.8)

                # cookies éventuels
                try:
                    btn = page.query_selector("button:has-text('Accept')") or page.query_selector("button:has-text('OK')")
                    if btn: btn.click()
                except:
                    pass

                # Titre annonce
                titre_annonce = ""
                try:
                    h1 = page.query_selector("h1")
                    titre_annonce = clean_text(h1.inner_text() if h1 else "")
                except:
                    pass

                # Code licence (précis)
                code_licence = extract_license_precise(page)

                # Profil hôte (nom + URL)
                url_profil_hote, nom_hote = "", ""
                try:
                    host_link = page.query_selector("a[href*='/users/show/']")
                    if host_link:
                        hp = (host_link.get_attribute("href") or "")
                        if hp.startswith("/"): hp = "https://www.airbnb.com" + hp
                        url_profil_hote = hp
                        txt = clean_text(host_link.inner_text() or "")
                        if txt:
                            nom_hote = txt
                except:
                    pass

                # NOTE GLOBALE DE L’HÔTE (sur la page PROFIL, pas l’annonce)
                note_globale_hote, nb_annonces_hote, date_inscription_hote = "", "", ""
                if url_profil_hote:
                    p2 = context.new_page()
                    p2.goto(url_profil_hote, timeout=90000)
                    pause(0.9, 1.5)

                    try:
                        body_txt = p2.inner_text("body")
                        m = re.search(r"([0-5]\.\d{1,2})\s*(?:out of 5|·|/5|rating|reviews)", body_txt, re.IGNORECASE)
                        if m:
                            note_globale_hote = m.group(1)
                    except:
                        pass

                    try:
                        cards = p2.query_selector_all("a[href*='/rooms/']")
                        nb_annonces_hote = str(len({(c.get_attribute('href') or '').split('?')[0] for c in cards}))
                    except:
                        pass

                    try:
                        joined = p2.query_selector("span:has-text('Joined')") or p2.query_selector("div:has-text('Joined')")
                        if joined:
                            date_inscription_hote = clean_text(joined.inner_text())
                    except:
                        pass

                    p2.close()

                rows.append({
                    "url_annonce": url,
                    "titre_annonce": titre_annonce,
                    "code_licence": code_licence,
                    "nom_hote": nom_hote,
                    "url_profil_hote": url_profil_hote,
                    "note_globale_hote": note_globale_hote,
                    "nb_annonces_hote": nb_annonces_hote,
                    "date_inscription_hote": date_inscription_hote,
                })
                print(f"[{idx}/{len(urls)}] OK — {titre_annonce} — licence:{code_licence} — note hôte:{note_globale_hote}")
            except Exception as e:
                print(f"[{idx}] ERREUR {url}: {e}")

        # CSV (UTF-8 avec BOM pour Excel)
        if rows:
            keys = list(rows[0].keys())
            with open(OUTPUT, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=keys)
                w.writeheader()
                for r in rows:
                    w.writerow(r)
            print("CSV écrit:", OUTPUT)
        else:
            print("Aucune donnée collectée.")
    print("FIN")

if __name__ == "__main__":
    main()
