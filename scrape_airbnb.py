import asyncio, csv, os, re, datetime
from pathlib import Path
from playwright.sync_api import sync_playwright

START_URL = os.getenv("START_URL")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", 10))
MAX_MINUTES = int(os.getenv("MAX_MINUTES", 10))

def uniq(seq):
    return list(dict.fromkeys([x.strip() for x in seq if x and x.strip()]))

def parse_license_code(text):
    """
    Extracts the registration/license code from a modal text by looking for
    lines immediately following 'Infos d\\'enregistrement' or 'Détails de l\\'enregistrement'.
    """
    for line in text.splitlines():
        if re.search(r"(Infos d['’]?enregistrement|Détails de l['’]?enregistrement)", line, re.I):
            continue  # skip the heading
        # look for codes with letters/numbers separated by hyphens or numeric codes
        m = re.search(r"[A-Z]{3}-[A-Z]{3}-[A-Z0-9]{4,}|\\b\\d{4,}\\b", line)
        if m:
            return m.group(0).strip()
    return ""

def scrape_listing(page, url, csv_writer):
    page.goto(url, timeout=60000)
    page.wait_for_load_state("domcontentloaded")

    title = page.title().strip() if page.title() else ""
    host_name = host_rating = host_profile_url = host_joined = license_code = ""

    # 1. Extract registration code from the "About" modal
    try:
        # Buttons labeled "Afficher plus", "Lire la suite", or similar open the modal.
        expand_buttons = page.locator("button:has-text('Afficher plus'), button:has-text('Lire la suite')")
        if expand_buttons.count() > 0:
            expand_buttons.first.click()
            # Wait for modal and fetch text
            modal = page.locator("div[role='dialog']").first
            modal.wait_for(state="visible", timeout=5000)
            modal_text = modal.inner_text()
            license_code = parse_license_code(modal_text)
            # Close modal by pressing Escape
            page.keyboard.press("Escape")
    except Exception as e:
        print(f"Failed to parse registration code for {url}: {e}")

    # 2. Scroll down to the host card and extract host info
    try:
        # Scroll until the host section appears
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        host_section = page.locator("section:has-text('Faites connaissance'), section:has-text('Meet your host')")
        if host_section.count() > 0:
            card = host_section.first
            # Host profile link
            profile_link = card.locator("a[href*='/users/show']").first
            if profile_link.count() > 0:
                host_profile_url = profile_link.get_attribute("href")
            # Host name
            name_elem = card.locator("h2, span").first
            host_name = name_elem.inner_text().strip() if name_elem.count() else ""
            # Host rating (extract numeric rating e.g. 4.93)
            rating_elem = card.locator("[data-star-rating], span:has-text('★')")
            if rating_elem.count() > 0:
                host_rating = re.search(r"\\d+(?:\\.\\d+)?", rating_elem.inner_text() or "").group(0)
            # Host joined year
            join_elem = card.locator("span:has-text('Depuis'), span:has-text('Since')")
            if join_elem.count() > 0:
                host_joined = re.search(r"\\d{4}", join_elem.inner_text() or "")
                host_joined = host_joined.group(0) if host_joined else ""
    except Exception as e:
        print(f"Failed to parse host info for {url}: {e}")

    scraped_at = datetime.datetime.utcnow().isoformat()
    csv_writer.writerow([url, title, license_code, host_name, host_rating, host_profile_url, host_joined, scraped_at])

def main():
    csv_path = Path("airbnb_results.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f, sync_playwright() as p:
        writer = csv.writer(f)
        writer.writerow(["url", "title", "license_code", "host_name",
                         "host_overall_rating", "host_profile_url",
                         "host_joined", "scraped_at"])
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            # Collect listing URLs
            page.goto(START_URL, timeout=60000)
            page.wait_for_load_state("domcontentloaded")
            urls = []
            scroll_height = 0
            while len(urls) < MAX_LISTINGS:
                page.evaluate("window.scrollBy(0, 1000)")
                page.wait_for_timeout(500)
                cards = page.locator("a[href*='/rooms/']")
                hrefs = uniq([href for href in cards.evaluate_all("els => els.map(e => e.href)")])
                for href in hrefs:
                    if href not in urls:
                        urls.append(href)
                        if len(urls) >= MAX_LISTINGS:
                            break
                new_scroll_height = page.evaluate("() => document.body.scrollHeight")
                if new_scroll_height == scroll_height:
                    break
                scroll_height = new_scroll_height

            # Scrape each listing
            for url in urls[:MAX_LISTINGS]:
                scrape_listing(page, url, writer)

        finally:
            browser.close()

if __name__ == "__main__":
    main()
