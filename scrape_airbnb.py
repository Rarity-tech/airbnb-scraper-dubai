# scrape_airbnb.py
#!/usr/bin/env python3
import os, csv, time, re, sys
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL   = os.getenv('START_URL', 'https://www.airbnb.com/s/Dubai/homes')
MAX_LISTINGS= int(os.getenv('MAX_LISTINGS','20'))
MAX_MINUTES = int(os.getenv('MAX_MINUTES','5'))
PROXY       = os.getenv('PROXY') or None

OUT = 'airbnb_results.csv'
HEADERS = [
    'url','title','license_code','host_name',
    'host_overall_rating','host_profile_url','host_joined','scraped_at'
]

def clean_url(href, base):
    if not href: return None
    u = urljoin(base, href)
    p = list(urlparse(u)); p[4] = ''; p[5] = ''
    return urlunparse(p)

def maybe_click(page, selector, timeout=2000):
    try:
        loc = page.locator(selector)
        if loc.first.is_visible(timeout=timeout):
            loc.first.click()
            return True
    except Exception:
        pass
    return False

def collect_listing_urls(page, target, deadline):
    urls, seen = [], set()
    while len(urls) < target and time.time() < deadline:
        for el in page.locator('a[href*="/rooms/"]').all():
            href = el.get_attribute('href')
            u = clean_url(href, page.url)
            if not u: continue
            if '/rooms/' in u and '/photos' not in u and '/reviews' not in u:
                if u not in seen:
                    seen.add(u); urls.append(u)
                    if len(urls) >= target: break
        page.mouse.wheel(0, 2500)
        page.wait_for_timeout(800)
    return urls

REG_LABEL = re.compile(
    r'(?:Infos?|D[ée]tails?)\s+de\s+l[\'’]enregistrement|registration|licen[cs]e|permit|enregistrement|رقم\s*(?:التسجيل|الترخيص)',
    re.I
)
REG_TOKEN = re.compile(r'\b([A-Z]{2,5}(?:-[A-Z0-9]{2,6}){1,6}|\d{5,})\b')

def extract_registration_code(page):
    opened = False
    for sel in [
        'button:has-text("Lire la suite")',
        'button:has-text("Afficher plus")',
        'button:has-text("Read more")',
        'button:has-text("Show more")'
    ]:
        if maybe_click(page, sel, timeout=1500):
            opened = True
            break
    dialog = page.locator('[role="dialog"]').first if opened else None
    candidates = []

    try:
        if dialog and dialog.is_visible(timeout=2000):
            txt = dialog.inner_text()
            mlabel = REG_LABEL.search(txt)
            if mlabel:
                after = txt.split(mlabel.group(0), 1)[-1]
                m = REG_TOKEN.search(after)
                if m: return m.group(1).strip()
            for m in REG_TOKEN.finditer(txt):
                candidates.append(m.group(1))
    except Exception:
        pass

    try:
        txt = page.inner_text('body')
        mlabel = REG_LABEL.search(txt)
        if mlabel:
            after = txt.split(mlabel.group(0), 1)[-1]
            m = REG_TOKEN.search(after)
            if m: return m.group(1).strip()
        for m in REG_TOKEN.finditer(txt):
            candidates.append(m.group(1))
    except Exception:
        pass

    return candidates[0] if candidates else ''

def extract_host_block(page):
    for sel in [
        'section:has(h2:has-text("Hôte"))',
        'section:has(h2:has-text("Host"))',
        '[data-section-id*="HOST"]',
    ]:
        try:
            loc = page.locator(sel).first
            if loc.count() and loc.is_visible(timeout=2000):
                return loc
        except PWTimeout:
            continue
    return page.locator('body')

def extract_host_info(page):
    host_section = extract_host_block(page)

    host_link = ''
    try:
        a = host_section.locator('a[href*="/users/show/"]').first
        if a and a.count():
            host_link = clean_url(a.get_attribute('href'), page.url)
    except Exception:
        pass

    name = ''
    try:
        h2 = host_section.locator('h2').first
        if h2 and h2.count():
            t = h2.inner_text().strip()
            t = re.sub(r'^(H[ôo]te\s*[:：]\s*|Host\s*[:：]\s*)', '', t, flags=re.I)
            name = t.strip(' ·|•')
    except Exception:
        pass

    rating = ''
    try:
        rtxt = host_section.inner_text()
        m = re.search(r'([0-9]\.?[0-9]?)\s*/\s*5', rtxt)
        if m: rating = m.group(1)
    except Exception:
        pass

    joined = ''
    try:
        jtxt = host_section.inner_text()
        m = re.search(r'(inscrit en|joined in)\s+(\d{4})', jtxt, re.I)
        if m: joined = m.group(2)
    except Exception:
        pass

    return name, rating, host_link, joined

def scrape_listing(ctx, url):
    page = ctx.new_page()
    try:
        page.goto(url, wait_until='domcontentloaded', timeout=60000)

        for sel in [
            'button:has-text("Tout accepter")',
            'button:has-text("Accepter tout")',
            'button:has-text("Accepter")',
            'button:has-text("Accept all")'
        ]:
            maybe_click(page, sel, timeout=1500)
        page.wait_for_timeout(800)

        try:
            title = page.locator('h1').first.inner_text().strip()
        except Exception:
            title = ''

        license_code = extract_registration_code(page)
        host_name, host_rating, host_profile, host_joined = extract_host_info(page)

        return {
            'url': clean_url(url, url),
            'title': title,
            'license_code': license_code,
            'host_name': host_name,
            'host_overall_rating': host_rating,
            'host_profile_url': host_profile,
            'host_joined': host_joined,
            'scraped_at': datetime.now(timezone.utc).isoformat()
        }
    finally:
        page.close()

def main():
    deadline = time.time() + MAX_MINUTES * 60

    with sync_playwright() as pw:
        launch = {'headless': True}
        if PROXY: launch['proxy'] = {'server': PROXY}
        browser = pw.chromium.launch(**launch)
        ctx = browser.new_context(locale='fr-FR')
        page = ctx.new_page()
        page.goto(START_URL, wait_until='domcontentloaded', timeout=60000)

        # seeds
        urls = []
        if os.path.exists('urls.txt'):
            with open('urls.txt') as f:
                urls = [l.strip() for l in f if l.strip()]
        if not urls:
            urls = collect_listing_urls(page, MAX_LISTINGS, deadline)

        print(f'FOUND_URLS {len(urls)}')

        rows = []
        for i, u in enumerate(urls, 1):
            if time.time() > deadline: break
            try:
                print(f'#{i} {u}')
                rows.append(scrape_listing(ctx, u))
            except Exception as e:
                print(f'ERROR {u} -> {e}', file=sys.stderr)

        with open(OUT, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=HEADERS)
            w.writeheader()
            for r in rows: w.writerow(r)

        print(f'SAVED {len(rows)} rows to {OUT}')
        browser.close()

if __name__ == '__main__':
    main()
