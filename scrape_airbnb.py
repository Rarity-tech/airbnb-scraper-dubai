#!/usr/bin/env python3
import os, csv, time, random
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL   = os.environ.get("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LIST    = int(os.environ.get("MAX_LISTINGS", "10"))
MAX_MINUTES = int(os.environ.get("MAX_MINUTES", "10"))
PROXY       = os.environ.get("PROXY", "").strip()
OUTPUT_CSV  = "airbnb_results.csv"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

def collect_listing_urls(page, target_count, deadline):
    urls = set()
    tries = 0
    while len(urls) < target_count and datetime.utcnow() < deadline and tries < 12:
        anchors = page.locator("a[href*='/rooms/']")
        n = anchors.count()
        for i in range(n):
            href = anchors.nth(i).get_attribute("href")
            if not href: continue
            if "/rooms/" not in href: continue
            if href.startswith("/"):
                href = "https://www.airbnb.com" + href
            urls.add(href.split("?")[0])
            if len(urls) >= target_count: break
        # scroll pour charger plus
        page.evaluate("() => window.scrollBy(0, document.body.scrollHeight)")
        time.sleep(1.2)
        tries += 1
    return list(urls)

def extract_listing(page):
    def txt(locator_css, timeout=1500):
        loc = page.locator(locator_css)
        return loc.first.inner_text(timeout=timeout).strip() if loc.count() else ""
    # Titre
    title = txt("h1")
    # Prix (heuristique)
    price = ""
    price_box = page.locator("[data-testid='book-it-default']")
    if price_box.count():
        price = price_box.inner_text(timeout=1500).strip()
    else:
        p = page.locator("div:has-text('AED')").first
        price = p.inner_text(timeout=1000).strip() if p.count() else ""
    # Hôte
    host = ""
    host_link = page.locator("a[href*='/users/']").first
    if host_link.count():
        try:
            host = host_link.inner_text(timeout=1500).strip()
        except Exception:
            host = ""
    # Note
    rating = ""
    r = page.locator("span[aria-label*='rated']").first
    if r.count():
        try:
            rating = r.inner_text(timeout=1200).strip()
        except Exception:
            rating = ""
    return title, price, host, rating

def main():
    deadline = datetime.utcnow() + timedelta(minutes=MAX_MINUTES)
    proxy_cfg = {"server": PROXY} if PROXY else None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
        ctx = browser.new_context(user_agent=UA, locale="en-US", proxy=proxy_cfg)
        page = ctx.new_page()
        page.set_default_timeout(20000)

        try:
            page.goto(START_URL, wait_until="domcontentloaded")
        except PWTimeout:
            pass

        # collecte des URLs
        listing_urls = collect_listing_urls(page, MAX_LIST, deadline)

        # CSV header
        header = ["url","title","price_text","host","rating","scraped_at"]
        new_file = not os.path.exists(OUTPUT_CSV)
        with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if new_file: w.writerow(header)

            count = 0
            for url in listing_urls:
                if datetime.utcnow() >= deadline: break
                try:
                    page.goto(url, wait_until="domcontentloaded")
                except PWTimeout:
                    continue
                time.sleep(random.uniform(0.8,1.6))
                title, price, host, rating = extract_listing(page)
                w.writerow([url, title, price, host, rating, datetime.utcnow().isoformat()])
                count += 1
                if count >= MAX_LIST: break
                time.sleep(random.uniform(1.0,2.0))

        browser.close()

if __name__ == "__main__":
    main()
