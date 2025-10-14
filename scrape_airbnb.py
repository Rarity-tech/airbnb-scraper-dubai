# scrape_airbnb.py
import os, re, csv, sys, asyncio, time
from datetime import datetime, timezone
from urllib.parse import urljoin
from playwright.async_api import async_playwright

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS= int(os.getenv("MAX_LISTINGS","50"))
MAX_MINUTES = int(os.getenv("MAX_MINUTES","10"))
PROXY       = os.getenv("PROXY") or None
OUT_CSV     = "airbnb_results.csv"

# --------- Helpers ----------
LABELS_REGNUM = [
    "Registration number","Registration Number","Registration no.","Registration No","Registration #",
    "License","Licence","License number","License Number","License No","License no.","Permit number",
    "DTCM","Tourism license","Tourism License","Tourism permit",
    "Numéro d’enregistrement","Numéro d'enregistrement","Infos d’enregistrement","Infos d'enregistrement",
    "Número de registro","N.º de registro","Número de licencia",
    "Registrierungsnummer","Lizenznummer",
    "Numero di registrazione","Numero licenza",
    "Номер регистрации",
    "رقم التسجيل","رقم الترخيص",
    "등록 번호","라이선스 번호",
    "登録番号","ライセンス番号"
]

HOST_SECTION_LABELS = [
    "Meet your host","Rencontrez votre hôte","Conoce a tu anfitrión","Lerne deinen Gastgeber kennen",
    "Incontra il tuo host","تعرف على المُضيف","호스트를 소개합니다","ホストの紹介"
]

ROOM_LINK_RE = re.compile(r"^/rooms/\d+")
SCORE_RE     = re.compile(r"(\d+(?:\.\d+)?)\s*(?:out of|sur|de)?\s*5", re.I)
REGVAL_RE    = re.compile(r"[A-Za-z0-9\-\._/]{4,}")  # permissif mais filtré

def now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

async def click_all_expanders(page):
    # Ouvre le contenu replié pour que les sections apparaissent
    labels = ["Show more","Afficher plus","Ver más","Mehr anzeigen","Mostra altro","عرض المزيد","더보기","もっと見る"]
    for lab in labels:
        try:
            btns = page.locator(f"button:has-text('{lab}')")
            count = await btns.count()
            for i in range(count):
                try:
                    await btns.nth(i).click(timeout=500)
                except: pass
        except: pass

async def dismiss_banners(page):
    # Cookie/consent
    texts = ["Accept","I agree","OK","Got it","Accepter","J’accepte","J'accepte","D’accord","D'accord",
             "Aceptar","Einverstanden","Ho capito","موافق","확인","同意する"]
    for t in texts:
        try:
            await page.get_by_role("button", name=re.compile(t, re.I)).click(timeout=800)
        except: pass

async def extract_registration_number(page):
    # Stratégie: trouver un label parmi LABELS_REGNUM puis lire la LIGNE immédiatement suivante.
    # On balaie plusieurs structures possibles.
    # 1) libellé dans un dt/dd, 2) libellé dans un div/p suivi d’un sibling, 3) table-like stacks.
    text_content = ""
    for lab in LABELS_REGNUM:
        # a) libellé exact à l'affichage
        try:
            labLoc = page.locator(f"xpath=//*[normalize-space()='{lab}']").first
            if await labLoc.count():
                # suivant visuel
                val = await page.evaluate("""
                    (node) => {
                      function nextText(n){
                        // cherche le prochain élément avec du texte non vide
                        let cur = n;
                        // regarde d'abord le sibling direct
                        if (cur && cur.parentElement){
                          let sibs = Array.from(cur.parentElement.children);
                          let idx = sibs.indexOf(cur);
                          for(let i=idx+1;i<sibs.length;i++){
                            const t = sibs[i].innerText && sibs[i].innerText.trim();
                            if (t) return t;
                          }
                        }
                        // sinon remonte et cherche proche
                        let el = n;
                        for(let k=0;k<3;k++){
                          if (!el) break;
                          el = el.parentElement;
                          if (!el) break;
                          const kids = Array.from(el.children);
                          for(const c of kids){
                            if (c===n) continue;
                            const t = c.innerText && c.innerText.trim();
                            if (t) return t;
                          }
                        }
                        // fallback: plus bas dans le DOM
                        let nx = n.nextElementSibling;
                        while(nx){
                          const t = nx.innerText && nx.innerText.trim();
                          if (t) return t;
                          nx = nx.nextElementSibling;
                        }
                        return "";
                      }
                      return nextText(node) || "";
                    }
                """, labLoc)
                text_content = (val or "").strip()
                if text_content:
                    break
        except: pass
        # b) libellé partiel inclus dans un conteneur
        try:
            labLoc2 = page.locator(f"xpath=//*[contains(normalize-space(), '{lab}')]").first
            if await labLoc2.count():
                val = await page.evaluate("""
                    (node) => {
                      // essaie sibling direct
                      let nx = node.nextElementSibling;
                      while(nx){
                        const t = nx.innerText && nx.innerText.trim();
                        if (t) return t;
                        nx = nx.nextElementSibling;
                      }
                      // sinon, enfants suivants
                      const p = node.parentElement;
                      if (p){
                        const kids = Array.from(p.children);
                        const idx = kids.indexOf(node);
                        for(let i=idx+1;i<kids.length;i++){
                          const t = kids[i].innerText && kids[i].innerText.trim();
                          if (t) return t;
                        }
                      }
                      return "";
                    }
                """, labLoc2)
                text_content = (val or "").strip()
                if text_content:
                    break
        except: pass

    if not text_content:
        return ""

    # Nettoyage: prendre le premier token plausible
    # Parfois Airbnb affiche "Permit: 12345" sur la ligne. On isole la partie code.
    # On enlève libellés résiduels et emojis.
    line = re.sub(r"[\u2600-\u27FF\uFE0F]", "", text_content).strip()
    # Couper aux séparateurs usuels si multichamps
    parts = re.split(r"[|\n\r•]+", line)
    cand = parts[0].strip() if parts else line

    # Extraire séquence alphanum/tirets/._/slash de >=4
    m = REGVAL_RE.search(cand)
    return m.group(0) if m else cand[:64]

async def extract_host_block(page):
    # Cherche la section "Meet your host"
    host_name = ""
    host_url  = ""
    host_rating = ""

    # Pré-étape: ouvrir expandeurs
    await click_all_expanders(page)

    # 1) section par titre
    hostSection = None
    for hlabel in HOST_SECTION_LABELS:
        try:
            heading = page.locator(f"xpath=//*[self::h1 or self::h2 or self::h3 or self::h4][normalize-space()='{hlabel}']").first
            if await heading.count():
                # remonter au conteneur
                hostSection = heading.locator("xpath=ancestor::*[1]")
                if await hostSection.count():
                    break
        except: pass

    # Fallback: rechercher un lien profil dans la page et prendre le bloc parent le plus proche d’un libellé
    if not hostSection:
        try:
            profile_link = page.locator("a[href^='/users/show/']").first
            if await profile_link.count():
                hostSection = profile_link.locator("xpath=ancestor::*[position()<=3]").last
        except: pass

    if hostSection:
        # Nom + URL
        try:
            a = hostSection.locator("a[href^='/users/show/']").first
            if await a.count():
                host_name = (await a.inner_text()).strip()
                rel = await a.get_attribute("href")
                if rel:
                    host_url = urljoin("https://www.airbnb.com", rel)
        except: pass

        # Note dans cette section
        try:
            # Cherche aria-label style "4.98 out of 5"
            star = hostSection.locator("[aria-label*='out of 5'], [aria-label*='sur 5'], [aria-label*='de 5']").first
            if await star.count():
                aria = await star.get_attribute("aria-label")
                if aria:
                    m = SCORE_RE.search(aria)
                    if m:
                        host_rating = m.group(1)
            if not host_rating:
                # Texte brut autour des étoiles
                txt = (await hostSection.inner_text()).strip()
                m2 = SCORE_RE.search(txt)
                if m2:
                    host_rating = m2.group(1)
                # éviter "New"
                if not host_rating and re.search(r"\bNew\b|\bNouveau\b|\bNuevo\b|\bNeu\b", txt, re.I):
                    host_rating = ""
        except: pass

    # Fallback: ouvrir profil si rating manquant mais nom+url dispo
    if not host_rating and host_url:
        host_rating = await fetch_host_rating_from_profile(page.context, host_url)

    return host_name, host_url, host_rating

async def fetch_host_rating_from_profile(context, url):
    try:
        p = await context.new_page()
        await p.goto(url, wait_until="domcontentloaded", timeout=30000)
        await dismiss_banners(p)
        await click_all_expanders(p)
        txt = await p.inner_text("body")
        m = SCORE_RE.search(txt)
        await p.close()
        return m.group(1) if m else ""
    except:
        return ""

async def collect_listing_links(page):
    # Défile et capture des liens /rooms/\d+
    seen = set()
    last_size = -1
    start = time.time()
    while True:
        # collect
        for a in await page.locator("a[href^='/rooms/']").all():
            try:
                href = await a.get_attribute("href")
                if not href: continue
                # normaliser
                href = href.split("?")[0]
                if ROOM_LINK_RE.match(href):
                    seen.add(urljoin(page.url, href))
            except: pass

        # stop conditions
        if len(seen) >= MAX_LISTINGS: break
        if time.time() - start > MAX_MINUTES*60: break
        if len(seen) == last_size:
            # scroll extra
            try:
                await page.mouse.wheel(0, 2000)
                await page.wait_for_timeout(800)
            except: pass
        last_size = len(seen)
        # auto scroll
        try:
            await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except: pass
        await page.wait_for_timeout(700)
    return list(seen)[:MAX_LISTINGS]

async def process_listing(context, url):
    page = await context.new_page()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await dismiss_banners(page)
        await click_all_expanders(page)

        # Titre
        try:
            title = (await page.title()).strip()
        except:
            title = ""

        # Hôte
        host_name, host_url, host_rating = await extract_host_block(page)

        # Code d’enregistrement
        license_code = await extract_registration_number(page)

        return {
            "url": url,
            "title": title,
            "license_code": license_code,
            "host_name": host_name,
            "host_overall_rating": host_rating,
            "host_profile_url": host_url,
            "host_joined": "",  # non demandé côté annonce; à remplir si besoin via profil
            "scraped_at": now_iso()
        }
    finally:
        await page.close()

async def main():
    launch_args = {
        "headless": True,
        "proxy": {"server": PROXY} if PROXY else None
    }
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**{k:v for k,v in launch_args.items() if v is not None})
        context = await browser.new_context(locale="fr-FR")  # peu importe, extraction multi-langue
        page = await context.new_page()
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=45000)
        await dismiss_banners(page)

        links = await collect_listing_links(page)

        rows = []
        for idx, url in enumerate(links, 1):
            try:
                rec = await process_listing(context, url)
                rows.append(rec)
                print(f"[{idx}/{len(links)}] {url} | host={rec['host_name']} | lic={rec['license_code']} | rating={rec['host_overall_rating']}")
            except Exception as e:
                print(f"[ERR] {url}: {e}", file=sys.stderr)

        await browser.close()

    # Écriture CSV sans prix
    fieldnames = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"SAVED {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
