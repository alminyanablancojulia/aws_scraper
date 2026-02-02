import re
import time
import random
import hashlib
from pathlib import Path
from urllib.parse import urlparse

import requests
import pandas as pd
from lxml import html, etree
from io import BytesIO


# =========================
# CONFIG
# =========================
SITEMAP_URL = "https://aws.amazon.com/marketplace/sitemap.xml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; thesis-research/1.0)",
    "Accept-Language": "en-US,en;q=0.9",
}

# Politeness / safety
BASE_DELAY = 1.6          # seconds
JITTER = 1.2              # random extra delay up to this
MAX_RETRIES = 5
TIMEOUT = 25

# Run settings
SAMPLE_PRODUCTS_TOTAL = 100   # increase when stable (e.g., 2000)
RANDOM_SEED = 42

# Output
OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

CACHE_DIR = OUT_DIR / "cache_html"
CACHE_DIR.mkdir(exist_ok=True)

TAXONOMY_FILE = OUT_DIR / "urls_taxonomy.csv"
PRODUCTS_FILE = OUT_DIR / "products_enriched_full.csv"

# Resume mode: skip prodviews already present in PRODUCTS_FILE
RESUME = True

# Extra safety pause every N products
PAUSE_EVERY_N = 100
PAUSE_SECONDS_RANGE = (30, 90)


# =========================
# HELPERS
# =========================
def polite_sleep(mult=1.0):
    time.sleep(mult * (BASE_DELAY + random.random() * JITTER))


def safe_filename(s: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", s)[:120]


def prodview_id_from_url(url: str) -> str | None:
    """
    Extract prodview-... from /marketplace/pp/prodview-xxxxx
    """
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if not parts:
        return None
    last = parts[-1]
    return last if last.startswith("prodview-") else None


def cache_path(kind: str, url: str) -> Path:
    """
    Cache by prodview id if possible, else by url hash.
    """
    pid = prodview_id_from_url(url)
    if pid:
        name = pid
    else:
        name = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{kind}__{safe_filename(name)}.html"


def fetch_url(session: requests.Session, url: str, kind: str, use_cache=True) -> bytes | None:
    """
    Safe fetch:
    - disk cache
    - NO retry on 404 (page doesn't exist)
    - retry with backoff for 429/5xx
    """
    cpath = cache_path(kind, url)
    if use_cache and cpath.exists():
        return cpath.read_bytes()

    backoff = 2.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            status = r.status_code

            # IMPORTANT: 404 is not transient -> do not retry
            if status == 404:
                print(f"[INFO] 404 (no page) {url}")
                return None

            # Rate limiting / transient errors -> retry
            if status in (429, 500, 502, 503, 504):
                print(f"[WARN] {status} {url} (attempt {attempt}/{MAX_RETRIES}) -> backoff {backoff:.1f}x")
                polite_sleep(mult=backoff)
                backoff *= 2
                continue

            r.raise_for_status()
            content = r.content

            if use_cache:
                cpath.write_bytes(content)

            polite_sleep()
            return content

        except Exception as e:
            print(f"[WARN] fetch failed {url} (attempt {attempt}/{MAX_RETRIES}): {e}")
            polite_sleep(mult=backoff)
            backoff *= 2

    print(f"[ERROR] FAILED: {url}")
    return None


# =========================
# SITEMAP
# =========================
def parse_sitemap(session: requests.Session) -> pd.DataFrame:
    print("[INFO] Fetching sitemap")
    content = fetch_url(session, SITEMAP_URL, kind="sitemap", use_cache=True)
    if not content:
        raise RuntimeError("Could not fetch sitemap")

    xml = etree.parse(BytesIO(content))
    urls = [el.text for el in xml.findall(".//{*}loc") if el.text]
    print(f"[INFO] {len(urls)} URLs found in sitemap")

    rows = []
    for url in urls:
        parts = urlparse(url).path.strip("/").split("/")
        if not parts or parts[0] != "marketplace":
            continue

        rows.append({
            "url": url,
            "section": parts[1] if len(parts) > 1 else None,
            "level_2": parts[2] if len(parts) > 2 else None,
            "level_3": parts[3] if len(parts) > 3 else None,
            "depth": len(parts) - 1
        })

    return pd.DataFrame(rows)


# =========================
# PRODUCT PAGE EXTRACTORS
# =========================
def extract_product_name(tree: html.HtmlElement) -> str | None:
    title = tree.xpath("//title/text()")
    if not title:
        return None
    t = title[0].strip()
    if t.lower().startswith("aws marketplace:"):
        return t.split(":", 1)[1].strip()
    return t


def extract_seller_name(tree: html.HtmlElement) -> str | None:
    sellers = tree.xpath("//a[contains(@href,'/marketplace/seller-profile')]/text()")
    sellers = [s.strip() for s in sellers if s and s.strip()]
    return sellers[0] if sellers else None


def extract_categories(tree: html.HtmlElement) -> tuple[str | None, str | None]:
    cats = tree.xpath("//a[contains(@href,'/marketplace/b/')]/text()")
    cats = [c.strip() for c in cats if c and c.strip()]
    primary = cats[0] if cats else None
    allcats = "|".join(cats) if cats else None
    return primary, allcats


def detect_delivery_method(page_text: str) -> str | None:
    t = page_text.lower()
    if "software as a service" in t or "(saas)" in t:
        return "SaaS"
    if "amazon machine image" in t or "(ami)" in t:
        return "AMI"
    if "container" in t and ("kubernetes" in t or "ecs" in t or "ecr" in t):
        return "Container"
    if "professional services" in t:
        return "Professional Services"
    if "data product" in t or "data exchange" in t or "data sets" in t:
        return "Data"
    return None


# =========================
# PRICING
# =========================
def classify_pricing(page_text: str) -> str:
    t = page_text.lower()

    if "free trial" in t:
        return "free_trial"
    if "bring your own license" in t or "byol" in t:
        return "byol"
    if "usage-based" in t or "usage based" in t:
        return "usage_based"
    if "cost/hour" in t or "hourly" in t:
        return "hourly"
    if "cost/month" in t:
        return "monthly"
    if "12-month contract" in t or "12 month contract" in t:
        return "contract"
    if "annual" in t:
        return "annual"
    if "contact seller" in t or ("contact" in t and "pricing" in t):
        return "contact_seller"

    return "unknown"


def parse_pricing_details(page_text: str) -> dict:
    out = {
        "pricing_type": classify_pricing(page_text),
        "contract_terms": None,
        "price_visible": 0,
        "price_min_usd": None,
        "price_max_usd": None
    }

    # contract terms e.g. "12-month contract"
    terms = sorted(set(re.findall(r"\b(\d+)\s*-\s*month contract\b", page_text, flags=re.IGNORECASE)))
    if not terms:
        terms = sorted(set(re.findall(r"\b(\d+)\s*month contract\b", page_text, flags=re.IGNORECASE)))

    if terms:
        out["contract_terms"] = ",".join([f"{t}-month" for t in terms])

    # parse USD prices
    prices = re.findall(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)", page_text)
    vals = []
    for p in prices:
        try:
            vals.append(float(p.replace(",", "")))
        except:
            pass

    if vals:
        out["price_visible"] = 1
        out["price_min_usd"] = min(vals)
        out["price_max_usd"] = max(vals)

    return out


# =========================
# REVIEWS (from reviews-list page)
# =========================
def parse_reviews_list_page(text: str) -> dict:
    t = re.sub(r"\s+", " ", text).strip()
    low = t.lower()

    out = {
        "reviews_page_exists": 1,
        "reviews_supported": 1,
        "avg_rating": None,
        "ratings_count": None,
        "aws_reviews_count": None,
        "external_reviews_count": None,
    }

    # Detect "not supported"
    if "reviews are not supported" in low:
        out["reviews_supported"] = 0
        return out

    def to_int(s):
        try:
            return int(s.replace(",", "").strip())
        except:
            return None

    # "3 AWS reviews"
    m = re.search(r"(\d[\d,]*)\s+AWS reviews\b", t, flags=re.IGNORECASE)
    if m:
        out["aws_reviews_count"] = to_int(m.group(1))

    # "965 external reviews"
    m = re.search(r"(\d[\d,]*)\s+external reviews\b", t, flags=re.IGNORECASE)
    if m:
        out["external_reviews_count"] = to_int(m.group(1))

    # "968 ratings"
    m = re.search(r"(\d[\d,]*)\s+ratings\b", t, flags=re.IGNORECASE)
    if m:
        out["ratings_count"] = to_int(m.group(1))

    # Average rating like "4.6 out of 5"
    m = re.search(r"\b([0-5]\.\d)\s+out of 5\b", t, flags=re.IGNORECASE)
    if m:
        try:
            out["avg_rating"] = float(m.group(1))
        except:
            pass

    return out


def fetch_reviews_info(session: requests.Session, prodview_id: str, use_cache=True) -> dict:
    reviews_url = f"https://aws.amazon.com/marketplace/reviews/reviews-list/{prodview_id}"
    content = fetch_url(session, reviews_url, kind="reviews", use_cache=use_cache)

    # None usually means 404 (no page) or hard failure
    if not content:
        return {
            "reviews_page_exists": 0,
            "reviews_supported": 0,
            "avg_rating": None,
            "ratings_count": None,
            "aws_reviews_count": None,
            "external_reviews_count": None,
        }

    tree = html.fromstring(content)
    text = tree.text_content()

    if len(text.strip()) < 50:
        return {
            "reviews_page_exists": 1,
            "reviews_supported": None,
            "avg_rating": None,
            "ratings_count": None,
            "aws_reviews_count": None,
            "external_reviews_count": None,
        }

    parsed = parse_reviews_list_page(text)
    parsed["reviews_page_exists"] = 1
    return parsed


# =========================
# ENRICH ONE PRODUCT
# =========================
def fetch_product_info(session: requests.Session, url: str, use_cache=True) -> dict | None:
    content = fetch_url(session, url, kind="product", use_cache=use_cache)
    if not content:
        return None

    tree = html.fromstring(content)
    text = re.sub(r"\s+", " ", tree.text_content()).strip()

    pid = prodview_id_from_url(url)
    if not pid:
        return None

    product_name = extract_product_name(tree)
    seller_name = extract_seller_name(tree)
    category_primary, categories_all = extract_categories(tree)
    delivery_method = detect_delivery_method(text)

    pricing = parse_pricing_details(text)
    reviews = fetch_reviews_info(session, pid, use_cache=use_cache)

    row = {
        "url": url,
        "prodview_id": pid,
        "product_name": product_name,
        "seller_name": seller_name,
        "category_primary": category_primary,
        "categories_all": categories_all,
        "delivery_method": delivery_method,
        **pricing,
        **reviews
    }

    print(
        f"[OK] {pid} | {product_name} | pricing={row['pricing_type']} "
        f"| aws_reviews={row['aws_reviews_count']} | ext_reviews={row['external_reviews_count']} "
        f"| reviews_page={row['reviews_page_exists']}"
    )
    return row


# =========================
# MAIN
# =========================
def main():
    random.seed(RANDOM_SEED)

    with requests.Session() as session:
        # 1) Parse sitemap
        taxonomy_df = parse_sitemap(session)
        taxonomy_df.to_csv(TAXONOMY_FILE, index=False)
        print(f"[INFO] Saved taxonomy -> {TAXONOMY_FILE}")

        # 2) Product URLs
        product_urls = taxonomy_df[taxonomy_df["section"] == "pp"]["url"].dropna().unique().tolist()
        print(f"[INFO] Found {len(product_urls)} product URLs")

        # 3) Sample
        if SAMPLE_PRODUCTS_TOTAL is not None:
            rng = random.Random(RANDOM_SEED)
            k = min(SAMPLE_PRODUCTS_TOTAL, len(product_urls))
            product_urls = rng.sample(product_urls, k=k)
            print(f"[INFO] Sampling {len(product_urls)} products")
        else:
            print("[INFO] Using ALL products (careful!)")

        # 4) Resume support
        done_ids = set()
        results = []

        if RESUME and PRODUCTS_FILE.exists():
            try:
                existing = pd.read_csv(PRODUCTS_FILE)
                if "prodview_id" in existing.columns:
                    done_ids = set(existing["prodview_id"].dropna().astype(str).unique().tolist())
                    results = existing.to_dict("records")
                    print(f"[INFO] Resume ON. Already have {len(done_ids)} products in {PRODUCTS_FILE}")
            except Exception as e:
                print(f"[WARN] Could not read resume file: {e}")

        # 5) Scrape
        for i, url in enumerate(product_urls, 1):
            pid = prodview_id_from_url(url)
            if pid and pid in done_ids:
                print(f"[SKIP] {i}/{len(product_urls)} {pid} already done")
                continue

            print(f"\n[{i}/{len(product_urls)}] Fetching product")
            row = fetch_product_info(session, url, use_cache=True)
            if row:
                results.append(row)
                # save continuously
                pd.DataFrame(results).to_csv(PRODUCTS_FILE, index=False)

            if PAUSE_EVERY_N and i % PAUSE_EVERY_N == 0:
                extra = random.randint(*PAUSE_SECONDS_RANGE)
                print(f"[PAUSE] Sleeping {extra}s for safety...")
                time.sleep(extra)

        print(f"\n[DONE] Saved final dataset -> {PRODUCTS_FILE}")


if __name__ == "__main__":
    main()
