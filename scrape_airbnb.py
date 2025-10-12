# scrape_airbnb.py
from playwright.sync_api import sync_playwright
import csv, re, time, random, os

# ----- CONFIG -----
LOCATION = os.getenv("LOCATION", "Dubai, United Arab Emirates")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "1000"))  # met 1000 (ou 500, 250 selon prudence)
OUTPUT = os.getenv("OUTPUT", "airbnb_dubai_listings.csv")
SCROLLS = int(os.getenv("SCROLLS", "20"))
# ------------------

LICENSE_REGEX = re.compile(r"(license|licence|registration|permit|trade\s*license|reg\.? no\.?)[:\s#-]*([A-Z0-9\-\/]{3,40})", re.IGNORECASE)

def extract_license(text):
    if not text: return ""
    for m in LICENSE_REGEX.finditer(text):
        return m.group(2)
    return ""

def random_sleep(a=1.0, b=2.5):
    time.sleep(random.uniform(a, b))

def main():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115 Safari/537.36")
        page = context.new_page()

        search_url = f"https://www.airbnb.com/s/{LOCATION.replace(' ', '%20')}/homes"
        print("Ouverture de :", search_url)
        page.goto(search_url, timeout=60000)
        random_sleep(2,4)

        listing_urls = []
        seen = set()
        for i in range(SCROLLS):
            # collect all links that look like listing links
            anchors = page.query_selector_all("a[href*='/rooms/'], a[href*='/listing/']")
            for a in anchors:
                href = a.get_attribute("href")
                if not href: continue
                if href.startswith("/"):
                    href = "https://www.airbnb.com" + href
                href = href.split("?")[0]
                if href not in seen:
                    seen.add(href)
                    listing_urls.append(href)
            print(f"Scroll {i+1}/{SCROLLS} — annonces trouvées: {len(listing_urls)}")
            if len(listing_urls) >= MAX_LISTINGS:
                break
            page.evaluate("window.scrollBy(0, document.body.scrollHeight);")
            random_sleep(1.5, 3.0)

        listing_urls = listing_urls[:MAX_LISTINGS]
        print("Total URLs à visiter:", len(listing_urls))

        rows = []
        idx = 0
        for url in listing_urls:
            idx += 1
            try:
                page.goto(url, timeout=60000)
                random_sleep(1.5, 3.5)
                # close cookie popup if present
                try:
                    btn = page.query_selector("button:has-text('Accept')") or page.query_selector("button:has-text('OK')")
                    if btn:
                        btn.click()
                        random_sleep(0.4, 0.8)
                except:
                    pass

                title = ""
                try:
                    h1 = page.query_selector("h1")
                    title = h1.inner_text().strip() if h1 else ""
                except:
                    title = ""

                # description block (heuristique)
                desc = ""
                try:
                    desc_el = page.query_selector("div[data-section-id='DESCRIPTION_DEFAULT']") or page.query_selector("div._1d7844ai") or page.query_selector("div:has-text('About this space')")
                    if desc_el:
                        desc = desc_el.inner_text().strip()
                except:
                    desc = ""

                license_num = extract_license(desc)

                host_profile = ""
                host_score = ""
                host_listings_count = ""
                host_join_info = ""

                try:
                    host_link_el = page.query_selector("a[href*='/users/show/']")
                    if host_link_el:
                        host_profile = host_link_el.get_attribute("href")
                        if host_profile.startswith("/"):
                            host_profile = "https://www.airbnb.com" + host_profile
                except:
                    pass

                # try rating on listing (overall rating)
                try:
                    rating_el = page.query_selector("span[aria-label^='Rated']") or page.query_selector("span[role='img'][aria-label*='rating']")
                    if rating_el:
                        host_score = rating_el.inner_text().strip()
                except:
                    pass

                # if host profile exists, open and extract listings count / joined info
                if host_profile:
                    page2 = context.new_page()
                    page2.goto(host_profile, timeout=45000)
                    random_sleep(1.0, 2.0)
                    try:
                        # look for "Listings" or number of properties
                        el = page2.query_selector("div:has-text('Listings')") or page2.query_selector("span:has-text('Listings')") or page2.query_selector("div._1g7m0tk")
                        if el:
                            host_listings_count = ''.join(ch for ch in el.inner_text() if ch.isdigit())
                    except:
                        pass
                    try:
                        joined = page2.query_selector("div:has-text('Joined')") or page2.query_selector("span:has-text('Joined')")
                        if joined:
                            host_join_info = joined.inner_text().strip()
                    except:
                        pass
                    page2.close()

                rows.append({
                    "url": url,
                    "title": title,
                    "license": license_num,
                    "host_profile": host_profile,
                    "host_score": host_score,
                    "host_listings_count": host_listings_count,
                    "host_join_info": host_join_info,
                    "description_snippet": (desc or "")[:500]
                })
                print(f"[{idx}] OK: {title} — license:{license_num} — score:{host_score}")
                random_sleep(1.0, 2.2)
            except Exception as e:
                print("Erreur sur", url, ":", str(e))
                random_sleep(2,4)
                continue

        if rows:
            keys = list(rows[0].keys())
            with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
                import csv
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                for r in rows:
                    writer.writerow(r)
            print("Fichier écrit:", OUTPUT)
        else:
            print("Aucune donnée collectée.")

if __name__=="__main__":
    main()
