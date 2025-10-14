# scrape_airbnb.py  — final robuste
import os, re, csv, asyncio, sys, time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit
from playwright.async_api import async_playwright

START_URL    = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "60"))
MAX_MINUTES  = int(os.getenv("MAX_MINUTES",  "10"))
PROXY        = os.getenv("PROXY") or None
OUT_CSV      = "airbnb_results.csv"

ROOM_RE   = re.compile(r"^/rooms/\d+")
RATING_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(?:out of|sur|de)?\s*5", re.I)

REG_LABELS = [
    # FR
    "Infos d’enregistrement","Infos d'enregistrement","Numéro d’enregistrement","Numéro d'enregistrement",
    # EN
    "Registration number","Registration Number","Registration no.","License","License number","Permit number",
    "Tourism license","Tourism permit",
    # ES/DE/IT/etc.
    "Número de registro","Número de licencia","Registrierungsnummer","Lizenznummer","Numero di registrazione",
    # AR/KO/JA (libellés fréquents)
    "رقم التسجيل","رقم الترخيص","등록 번호","ライセンス番号","登録番号"
]

HOST_SECTION_TITLES = [
    "Meet your host","Rencontrez votre hôte","Conoce a tu anfitrión","Lerne deinen Gastgeber kennen",
    "Incontra il tuo host","تعرف على المُضيف","호스트를 소개합니다","ホストの紹介"
]

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def normalize_abs(href: str) -> str:
    # Force en https://www.airbnb.com/... pour uniformiser
    try:
        u = urlsplit(urljoin("https://www.airbnb.com", href))
        return urlunsplit(("https", "www.airbnb.com", u.path, "", ""))
    except:
        return href

async def dismiss(page):
    # Ferme consentements
    labels = ["Accepter","J'accepte","J’accepte","OK","D'accord","Got it","Accept","I agree","Aceptar","Einverstanden","Ho capito","موافق","확인","同意する"]
    for t in labels:
        try:
            await page.get_by_role("button", name=re.compile(t, re.I)).click(timeout=800)
        except: pass

async def expand(page):
    # Ouvre les « Afficher plus »
    more = ["Afficher plus","Show more","Ver más","Mehr anzeigen","Mostra altro","عرض المزيد","더보기","もっと見る"]
    for t in more:
        try:
            btns = page.locator(f"button:has-text('{t}')")
            for i in range(await btns.count()):
                try: await btns.nth(i).click(timeout=500)
                except: pass
        except: pass

async def collect_room_links(page):
    seen, last = set(), -1
    start = time.time()
    while True:
        for a in await page.locator("a[href^='/rooms/']").all():
            try:
                href = (await a.get_attribute("href")) or ""
                href = href.split("?")[0]
                if ROOM_RE.match(href):
                    seen.add(normalize_abs(href))
            except: pass
        if len(seen) >= MAX_LISTINGS: break
        if time.time() - start > MAX_MINUTES*60: break
        if len(seen) == last:
            try: await page.mouse.wheel(0, 2000)
            except: pass
            await page.wait_for_timeout(600)
        last = len(seen)
        try: await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except: pass
        await page.wait_for_timeout(600)
    return list(seen)[:MAX_LISTINGS]

async def read_registration_number(page):
    # Cherche un libellé puis prend l'élément **visuellement suivant**
    for lab in REG_LABELS:
        # cible exact
        try:
            node = page.locator(f"xpath=//*[normalize-space()='{lab}']").first
            if await node.count():
                text = await page.evaluate("""
                    (n)=>{
                      // sibling direct puis premier descendant textuel non vide
                      function nextText(el){
                        // 1) sibling direct
                        let s = el.nextElementSibling;
                        while(s){
                          const t = (s.innerText||"").trim();
                          if(t) return t;
                          s = s.nextElementSibling;
                        }
                        // 2) même parent: élément suivant
                        const p = el.parentElement;
                        if(p){
                          const kids=[...p.children];
                          const i=kids.indexOf(el);
                          for(let k=i+1;k<kids.length;k++){
                            const t=(kids[k].innerText||"").trim();
                            if(t) return t;
                          }
                        }
                        // 3) descendant immédiat du parent suivant
                        let ps = el.parentElement?.nextElementSibling;
                        while(ps){
                          const t=(ps.innerText||"").trim();
                          if(t) return t;
                          ps = ps.nextElementSibling;
                        }
                        return "";
                      }
                      return nextText(n);
                    }
                """, node)
                val = (text or "").strip()
                if val:
                    # Si la ligne contient « : », on prend l’après-colon
                    val = val.split(":",1)[-1].strip()
                    # Prend le premier token plausible
                    m = re.search(r"[A-Za-z0-9][A-Za-z0-9\-/_. ]{2,}", val)
                    if m:
                        code = m.group(0).strip()
                        # coupe fin de phrase éventuelle
                        code = re.split(r"[\n\r•|]+", code)[0].strip()
                        return code[:80]
        except: pass
        # cible contenant
        try:
            node = page.locator(f"xpath=//*[contains(normalize-space(), '{lab}')]").first
            if await node.count():
                txt = await page.evaluate("(n)=>n?.parentElement?.innerText||n.innerText||''", node)
                # exemple: "Infos d'enregistrement\nBUR-BUR-OFXPS"
                for line in (txt or "").splitlines():
                    if line.strip() and lab.lower() not in line.lower():
                        cand = line.strip()
                        m = re.search(r"[A-Za-z0-9][A-Za-z0-9\-/_. ]{2,}", cand)
                        if m:
                            return m.group(0).strip()[:80]
        except: pass
    return ""

async def read_host_block(page):
    # 1) isole la **vraie** section hôte (évite les avis)
    host_container = None
    for title in HOST_SECTION_TITLES:
        try:
            h = page.locator(f"xpath=//*[self::h1 or self::h2 or self::h3][normalize-space()='{title}']").first
            if await h.count():
                host_container = h.locator("xpath=ancestor::*[1]")
                break
        except: pass
    if not host_container:
        # fallback: conteneur du premier lien profil le plus haut sur la page hors reviews
        try:
            host_link_any = page.locator("section:has(h2:has-text('host')), section:has(h3:has-text('host'))").locator("a[href^='/users/show/']").first
            if await host_link_any.count():
                host_container = host_link_any.locator("xpath=ancestor::*[position()<=3]").last
        except: pass

    host_name, host_url, host_rating = "", "", ""
    if host_container:
        # URL profil — limité à la section hôte
        try:
            a = host_container.locator("a[href^='/users/show/']").first
            if await a.count():
                href = await a.get_attribute("href")
                if href: host_url = normalize_abs(href)
        except: pass

        # Nom — dans la même zone
        try:
            # souvent « Hosted by <name> » ou nom près de l’avatar
            text_zone = await host_container.inner_text()
            m = re.search(r"(?:Hosted by|Hôte :|Host:)\s*(.+)", text_zone, re.I)
            if m:
                host_name = m.group(1).split("\n")[0].strip()
            if not host_name:
                # prend texte du lien profil s'il porte le nom
                t = ""
                try:
                    t = (await host_container.locator("a[href^='/users/show/']").first.inner_text()).strip()
                except: pass
                if t and len(t.split())<=5: host_name = t
            # Nettoyage
            host_name = re.sub(r"\s{2,}", " ", host_name).strip()
        except: pass

        # Note de l'hôte — **uniquement** dans cette section
        try:
            # aria-label "4.98 out of 5"
            star = host_container.locator("[aria-label*='out of 5'], [aria-label*='sur 5'], [aria-label*='de 5']").first
            if await star.count():
                aria = await star.get_attribute("aria-label")
                if aria:
                    m = RATING_RE.search(aria)
                    if m: host_rating = m.group(1)
            if not host_rating:
                txt = await host_container.inner_text()
                m = RATING_RE.search(txt)
                if m: host_rating = m.group(1)
        except: pass

    # Fallback profil si rating manquant et URL dispo
    if not host_rating and host_url:
        try:
            p = await page.context.new_page()
            await p.goto(host_url, wait_until="domcontentloaded", timeout=30000)
            await dismiss(p)
            txt = await p.inner_text("body")
            m = RATING_RE.search(txt)
            if m: host_rating = m.group(1)
            if not host_name:
                # essaie d'extraire le nom affiché dans le profil
                m2 = re.search(r"^([A-ZÀ-ÖØ-Ý][^\n]{1,40})\n", txt, re.M)
                if m2: host_name = m2.group(1).strip()
            await p.close()
        except: pass

    return host_name, host_url, host_rating

async def process_listing(context, url):
    p = await context.new_page()
    try:
        await p.goto(url, wait_until="domcontentloaded", timeout=45000)
        await dismiss(p); await expand(p)

        title = (await p.title()) or ""
        host_name, host_url, host_rating = await read_host_block(p)
        license_code = await read_registration_number(p)

        print(f"{url} | host={host_name} | lic={license_code} | rating={host_rating}")
        return {
            "url": url,
            "title": title.strip(),
            "license_code": license_code.strip(),
            "host_name": host_name.strip(),
            "host_overall_rating": host_rating.strip(),
            "host_profile_url": host_url.strip(),
            "host_joined": "",
            "scraped_at": now_utc_iso(),
        }
    finally:
        await p.close()

async def main():
    launch = {"headless": True}
    if PROXY: launch["proxy"] = {"server": PROXY}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch)
        context = await browser.new_context(locale="fr-FR")
        page = await context.new_page()
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=45000)
        await dismiss(page)

        links = await collect_room_links(page)
        rows = []
        for i, u in enumerate(links, 1):
            try:
                rows.append(await process_listing(context, u))
            except Exception as e:
                print(f"[ERR] {u} -> {e}", file=sys.stderr)
        await browser.close()

    fields = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)
    print(f"SAVED {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
