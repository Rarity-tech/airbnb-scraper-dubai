# scrape_airbnb.py
import os, csv, json, re, time
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

START_URL   = os.getenv("START_URL", "https://www.airbnb.com/s/Dubai/homes")
MAX_LISTINGS = int(os.getenv("MAX_LISTINGS", "50"))
MAX_MINUTES  = int(os.getenv("MAX_MINUTES", "10"))
PROXY        = os.getenv("PROXY", "").strip() or None

CSV_PATH = "airbnb_results.csv"

def deep_find(obj, keys):
    """Return first value for any of keys found anywhere in nested dict/list."""
    stack = [obj]
    seen = set()
    while stack:
        cur = stack.pop()
        if id(cur) in seen: 
            continue
        seen.add(id(cur))
        if isinstance(cur, dict):
            for k, v in cur.items():
                lk = k.lower()
                if lk in keys:
                    return v
            stack.extend(cur.values())
        elif isinstance(cur, list):
            stack.extend(cur)
    return None

def get_next_data_html(html):
    m = re.search(r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S | re.I)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return None

def get_ld_json_all(html):
    out = []
    for m in re.finditer(r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>', html, re.S | re.I):
        try:
            block = json.loads(m.group(1).strip())
            out.append(block)
        except Exception:
            continue
    return out

def clean(s):
    if not s:
        return ""
    # Collapse whitespace and long CTAs
    s = re.sub(r'\s+', ' ', str(s)).strip()
    return s[:1000]

def collect_listing_urls(page, limit):
    urls = []
    seen = set()
    start = time.time()
    while len(urls) < limit and (time.time() - start) < MAX_MINUTES * 60:
        page.wait_for_timeout(400)
        for a in page.locator('a[href*="/rooms/"]').all():
            try:
                href = a.get_attribute("href")
                if not href:
                    continue
                # unify and filter
                if "/rooms/" in href:
                    full = urljoin("https://www.airbnb.com/", href.split("?")[0])
                    if full not in seen:
                        seen.add(full)
                        urls.append(full)
                        if len(urls) >= limit:
                            break
            except Exception:
                continue
        # scroll to load more
        page.mouse.wheel(0, 2000)
    return urls[:limit]

def parse_from_structured(html):
    data = {
        "license_code": "",
        "host_name": "",
        "host_profile_url": "",
        "host_overall_rating": "",
        "host_joined": "",
        "title": "",
        "price_text": "",
    }

    next_data = get_next_data_html(html)
    if next_data:
        # title
        name = deep_find(next_data, {"name"})
        if not name:
            name = deep_find(next_data, {"listingname", "listing_name"})
        data["title"] = clean(name)

        # license
        lic = deep_find(next_data, {"license"})
        data["license_code"] = clean(lic)

        # host info
        host_name = deep_find(next_data, {"hostname", "host_name"})
        if not host_name:
            # sometimes under "user" object
            host_name = deep_find(next_data, {"displayname", "hostdisplayname"})
        data["host_name"] = clean(host_name)

        # overall rating
        rating = deep_find(next_data, {"overallrating", "starrating", "averagerating"})
        try:
            if isinstance(rating, dict) and "value" in rating:
                rating = rating["value"]
        except Exception:
            pass
        data["host_overall_rating"] = clean(rating)

        # host joined
        joined = deep_find(next_data, {"hostsince", "membersince", "member_since"})
        data["host_joined"] = clean(joined)

        # host profile url
        profile_path = deep_find(next_data, {"profilepath"})
        host_id = deep_find(next_data, {"hostid", "userid", "user_id"})
        if profile_path:
            data["host_profile_url"] = urljoin("https://www.airbnb.com", profile_path)
        elif host_id:
            try:
                hid = str(host_id).split("?")[0]
                data["host_profile_url"] = f"https://www.airbnb.com/users/show/{hid}"
            except Exception:
                pass

        # price text (fallback best-effort)
        price = deep_find(next_data, {"price", "displayprice", "priceitemdisplay"})
        data["price_text"] = clean(price)

    # LD+JSON fallback
    if not data["title"] or not data["host_name"] or not data["host_overall_rating"]:
        for block in get_ld_json_all(html):
            if isinstance(block, dict):
                if not data["title"]:
                    data["title"] = clean(block.get("name") or block.get("headline"))
                if not data["license_code"]:
                    data["license_code"] = clean(block.get("license"))
                aggr = block.get("aggregateRating") or {}
                if not data["host_overall_rating"]:
                    data["host_overall_rating"] = clean(aggr.get("ratingValue"))
                offers = block.get("offers") or {}
                if not data["price_text"]:
                    data["price_text"] = clean(offers.get("price") or offers.get("priceSpecification", {}).get("price"))

    return data

def main():
    results = []
    with sync_playwright() as p:
        launch_args = {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
            "proxy": {"server": PROXY} if PROXY else None,
        }
        browser = p.chromium.launch(**{k:v for k,v in launch_args.items() if v is not None})
        context = browser.new_context(
            locale="fr-FR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width":1280, "height":900}
        )
        page = context.new_page()
        page.set_default_timeout(15000)

        # Page de recherche
        page.goto(START_URL, wait_until="domcontentloaded")
        # bannière cookies éventuelle
        try:
            page.get_by_role("button", name=re.compile("Accepter|Accept", re.I)).click(timeout=3000)
        except Exception:
            pass

        urls = collect_listing_urls(page, MAX_LISTINGS)

        start = time.time()
        for url in urls:
            if (time.time() - start) > MAX_MINUTES * 60:
                break
            row = {
                "url": url,
                "title": "",
                "license_code": "",
                "host_name": "",
                "host_overall_rating": "",
                "host_profile_url": "",
                "host_joined": "",
                "price_text": "",
                "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            try:
                page.goto(url, wait_until="domcontentloaded")
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except PWTimeout:
                    pass

                html = page.content()
                data = parse_from_structured(html)

                for k in ["title","license_code","host_name","host_overall_rating","host_profile_url","host_joined","price_text"]:
                    row[k] = data.get(k, "") or row[k]

                # Titre de secours
                if not row["title"]:
                    try:
                        row["title"] = clean(page.title())
                    except Exception:
                        pass

            except Exception:
                # on garde l’URL et la date pour diagnostic
                pass

            results.append(row)

        browser.close()

    # Écriture CSV avec BOM pour Excel
    fieldnames = [
        "url","title","license_code","host_name","host_overall_rating",
        "host_profile_url","host_joined","price_text","scraped_at"
    ]
    with open(CSV_PATH, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(results)

if __name__ == "__main__":
    main()
