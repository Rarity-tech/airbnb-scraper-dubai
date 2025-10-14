# scrape_airbnb.py — clean room, robust, host-only selectors, multiline locales
import os, re, csv, asyncio, time, random, sys
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit, urlunsplit
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

START_URL    = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "60"))
MAX_MINUTES  = int(os.getenv("MAX_MINUTES",  "10"))
PROXY        = os.getenv("PROXY") or None
OUT_CSV      = "airbnb_results.csv"

ROOM_RE = re.compile(r"^/rooms/\d+")
# rating: 4.98, 4,98, 5
RATING_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:out of|sur|de)?\s*5", re.I)

# Libellés possibles pour ouvrir ou titrer le panneau "À propos / About"
ABOUT_TITLES = [
    "À propos de ce logement","A propos de ce logement","About this place","Acerca de este alojamiento",
    "Über diese Unterkunft","Informazioni su questo alloggio","حول مكان الإقامة هذا","이 숙소에 대해",
    "この宿泊先について","このお部屋について","この施設について","この宿について"
]

# Libellés qui précèdent le code d’enregistrement
REG_LABELS = [
    # FR
    "Infos d’enregistrement","Infos d'enregistrement","Numéro d’enregistrement","Numéro d'enregistrement",
    "Numéro d’enregistrement DTCM","Numéro de permis","Permis","Licence","N° d’enregistrement","N° de licence",
    # EN
    "Registration number","Registration Number","Registration no.","License number","Licence number",
    "Permit number","Tourism license","Tourism permit","License","Licence",
    # ES/DE/IT/PT
    "Número de registro","Número de licencia","Registrierungsnummer","Lizenznummer","Numero di registrazione",
    "Número da licença","Número de licença",
    # AR/KO/JA/ZH (courants)
    "رقم التسجيل","رقم الترخيص","رقم الرخصة","رقم تصريح","등록 번호","사업자 등록번호","ライセンス番号","登録番号","許可番号","许可证号"
]

# Titres de la section hôte pour isoler du reste (évite les commentateurs)
HOST_SECTION_TITLES = [
    "Meet your host","Rencontrez votre hôte","Conoce a tu anfitrión","Lerne deinen Gastgeber kennen",
    "Incontra il tuo host","Conheça o seu anfitrião","تعرف على المُضيف","호스트를 소개합니다",
    "ホストの紹介","このホストについて"
]

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def canon_abs(href:str)->str:
    try:
        u = urlsplit(urljoin("https://www.airbnb.com", href))
        return urlunsplit(("https","www.airbnb.com",u.path,"",""))
    except:
        return href or ""

async def maybe_click(page, locator, timeout=1200):
    try:
        await locator.click(timeout=timeout)
        return True
    except:
        return False

async def dismiss_noise(page):
    # cookies, modals, app banners
    texts = [
        "Accepter","J'accepte","J’accepte","OK","D'accord","Fermer","Close","Got it","Accept",
        "Aceptar","Einverstanden","Ho capito","موافق","확인","閉じる","同意"
    ]
    for t in texts:
        try:
            await page.get_by_role("button", name=re.compile(rf"^{re.escape(t)}$", re.I)).first.click(timeout=800)
        except: pass

async def expand_more(page):
    labels = ["Afficher plus","Show more","Ver más","Mehr anzeigen","Mostra altro","عرض المزيد","더보기","もっと見る"]
    for t in labels:
        try:
            btns = page.locator(f"button:has-text('{t}')")
            n = await btns.count()
            for i in range(n):
                try: await btns.nth(i).click(timeout=600)
                except: pass
        except: pass

async def collect_room_links(page):
    start = time.time()
    seen, last = set(), -1
    while True:
        for a in await page.locator("a[href^='/rooms/']").all():
            try:
                href = (await a.get_attribute("href")) or ""
                href = href.split("?")[0]
                if ROOM_RE.match(href): seen.add(canon_abs(href))
            except: pass
        if len(seen)>=MAX_LISTINGS or time.time()-start>MAX_MINUTES*60: break
        if len(seen)==last:
            try: await page.mouse.wheel(0, 2200)
            except: pass
            await page.wait_for_timeout(700)
        last = len(seen)
        try: await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        except: pass
        await page.wait_for_timeout(700)
    return list(seen)[:MAX_LISTINGS]

async def open_about_modal(page):
    # essaie plusieurs emplacements
    for t in ABOUT_TITLES:
        try:
            # bouton ou lien visible
            btn = page.locator(f"xpath=//*[self::button or self::a][normalize-space()='{t}']").first
            if await btn.count():
                if await maybe_click(page, btn):
                    # attends un dialog
                    try:
                        await page.wait_for_selector("[role='dialog'], div[aria-modal='true']", timeout=3000)
                    except: pass
                    return True
        except: pass
    return False

async def close_modal(page):
    try:
        # X de fermeture
        xbtn = page.locator("[role='dialog'] button:has-text('×'), [role='dialog'] button[aria-label*='Fermer'], [role='dialog'] button[aria-label*='Close']").first
        if await xbtn.count(): await xbtn.click(timeout=500)
    except: pass
    try: await page.keyboard.press("Escape")
    except: pass

async def extract_registration(page):
    # 1) tente dans le modal "À propos"
    got_modal = await open_about_modal(page)
    scopes = []
    if got_modal:
        scopes.append(page.locator("[role='dialog'], div[aria-modal='true']").first)
    # 2) fallback: corps de page
    scopes.append(page.locator("body"))

    for scope in scopes:
        # a) libellé exact puis **élément suivant**
        for lab in REG_LABELS:
            try:
                node = scope.locator(f"xpath=.//*[normalize-space()='{lab}']").first
                if await node.count():
                    txt = await page.evaluate("""
                        (n)=>{
                          function firstNonEmptyText(el){
                            const t=(el.innerText||"").trim();
                            if(t) return t;
                            for(const c of el.children||[]){
                              const r=firstNonEmptyText(c); if(r) return r;
                            }
                            return "";
                          }
                          // 1) élément suivant dans le même conteneur
                          let s=n.nextElementSibling;
                          while(s){ const t=firstNonEmptyText(s); if(t) return t; s=s.nextElementSibling; }
                          // 2) suivant du parent
                          let p=n.parentElement?.nextElementSibling;
                          while(p){ const t=firstNonEmptyText(p); if(t) return t; p=p.nextElementSibling; }
                          return "";
                        }
                    """, node)
                    val = (txt or "").strip()
                    if val:
                        val = val.split(":",1)[-1].strip()
                        m = re.search(r"[A-Za-z0-9][A-Za-z0-9\-_/\. ]{2,}", val)
                        if m:
                            code = m.group(0).strip()
                            code = re.split(r"[\n\r•|،。]+", code)[0].strip()
                            if got_modal: await close_modal(page)
                            return code[:80]
            except: pass
        # b) libellé inclus dans un bloc, puis prendre la **ligne suivante**
        try:
            alltxt = await scope.inner_text()
            for lab in REG_LABELS:
                pat = re.compile(rf"{re.escape(lab)}\s*[\n\r]+([^\n\r]+)", re.I)
                m = pat.search(alltxt)
                if m:
                    code = m.group(1).strip()
                    code = code.split(":",1)[-1].strip()
                    code = re.split(r"[\n\r•|،。]+", code)[0].strip()
                    if got_modal: await close_modal(page)
                    return code[:80]
        except: pass

    if got_modal: await close_modal(page)
    return ""

async def find_host_container(page):
    # titre exact
    for t in HOST_SECTION_TITLES:
        try:
            h = page.locator(f"xpath=//*[self::h1 or self::h2 or self::h3][normalize-space()='{t}']").first
            if await h.count():
                return h.locator("xpath=ancestor::*[self::section or self::div][1]")
        except: pass
    # titre partiel
    try:
        h = page.locator("xpath=//*[self::h1 or self::h2 or self::h3][contains(translate(., 'HOSTÉOÜÄÖÂÀÉÈÊÎÏÔÛÇabcdefghijklmnopqrstuvwxyz', 'hosteouaoaaeeeiio ucABCDEFGHIJKLMNOPQRSTUVWXYZ'),'HOST')]").first
        if await h.count():
            return h.locator("xpath=ancestor::*[self::section or self::div][1]")
    except: pass
    return None

def clean_rating(val:str)->str:
    if not val: return ""
    v = val.strip().replace(",", ".")
    m = RATING_RE.search(v)
    if m:
        d = m.group(1).replace(",", ".")
        try:
            # clamp to 0..5
            f = max(0.0, min(5.0, float(d)))
            return f"{f:.2f}".rstrip("0").rstrip(".")
        except: return d
    return ""

async def extract_host_info(page):
    host_name, host_url, host_rating = "", "", ""
    container = await find_host_container(page)
    if container:
        # lien profil uniquement dans ce conteneur
        try:
            a = container.locator("a[href^='/users/show/']").first
            if await a.count():
                href = await a.get_attribute("href")
                host_url = canon_abs(href or "")
                # texte du lien parfois = nom
                t = (await a.inner_text() or "").strip()
                if t and len(t.split())<=6: host_name = t
        except: pass

        # nom via texte "Hosted by <name>"
        try:
            block = await container.inner_text()
            m = re.search(r"(?:Hosted by|Hôte :|Host:)\s*([^\n\r•|]{1,80})", block, re.I)
            if m and not host_name:
                host_name = m.group(1).strip()
        except: pass

        # note de l’hôte (dans la même zone)
        try:
            star = container.locator("[aria-label*='out of 5'], [aria-label*='sur 5'], [aria-label*='de 5']").first
            if await star.count():
                aria = await star.get_attribute("aria-label")
                host_rating = clean_rating(aria or "")
            if not host_rating:
                host_rating = clean_rating(await container.inner_text())
        except: pass

    # fallback minimal si rating manquant mais URL dispo: aller sur profil
    if host_url and not host_rating:
        try:
            p = await page.context.new_page()
            await p.goto(host_url, wait_until="domcontentloaded", timeout=30000)
            await dismiss_noise(p)
            txt = await p.inner_text("body")
            host_rating = clean_rating(txt)
            if not host_name:
                # première ligne avec capitalisation
                m = re.search(r"^\s*([A-ZÀ-ÖØ-Ý][^\n\r]{1,40})\s*$", txt, re.M)
                if m: host_name = m.group(1).strip()
            await p.close()
        except: pass

    # nettoyage
    host_name = re.sub(r"\s{2,}", " ", host_name or "").strip()
    return host_name, host_url, host_rating

async def scrape_listing(context, url):
    p = await context.new_page()
    # ralentissement léger pour paraître humain
    await p.route("**/*", lambda r: r.continue_())
    try:
        await p.goto(url, wait_until="domcontentloaded", timeout=45000)
        await dismiss_noise(p)
        await expand_more(p)

        title = (await p.title() or "").strip()
        host_name, host_url, host_rating = await extract_host_info(p)
        license_code = await extract_registration(p)

        print(f"{url} | host={host_name} | lic={license_code} | rating={host_rating}")
        return {
            "url": url,
            "title": title,
            "license_code": license_code,
            "host_name": host_name,
            "host_overall_rating": host_rating,
            "host_profile_url": host_url,
            "host_joined": "",
            "scraped_at": now_utc(),
        }
    finally:
        await p.close()

async def main():
    launch = {
        "headless": True,
        "args": [
            "--no-sandbox","--disable-dev-shm-usage",
            "--disable-blink-features=AutomationControlled",
        ],
    }
    if PROXY: launch["proxy"] = {"server": PROXY}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch)
        context = await browser.new_context(
            locale="fr-FR",
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 824},
        )
        page = await context.new_page()
        await page.goto(START_URL, wait_until="domcontentloaded", timeout=45000)
        await dismiss_noise(page)

        links = await collect_room_links(page)
        rows = []
        start = time.time()
        for i, u in enumerate(links, 1):
            if time.time() - start > MAX_MINUTES*60: break
            try:
                rows.append(await scrape_listing(context, u))
            except PWTimeout:
                print(f"[TIMEOUT] {u}", file=sys.stderr)
            except Exception as e:
                print(f"[ERR] {u} -> {e}", file=sys.stderr)
            await asyncio.sleep(random.uniform(0.6, 1.4))  # throttle

        await browser.close()

    fields = ["url","title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","scraped_at"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)
    print(f"SAVED {len(rows)} rows to {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
