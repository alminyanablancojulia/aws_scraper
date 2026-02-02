import re
import time
import random
from io import BytesIO
from urllib.parse import urlparse

import requests
import pandas as pd
from lxml import etree, html


# =========================
# CONFIG (easy to change)
# =========================
SITEMAP_URL = "https://aws.amazon.com/marketplace/sitemap.xml"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
}

DELAY_SECONDS = 1.5          # base delay between requests
JITTER_SECONDS = 1.0         # random extra delay
TIMEOUT = 25

MAX_RETRIES = 3              # retry only for transient errors
SAMPLE_PRODUCTS_TOTAL = 300  # testing (increase to 1000/2000 later)
RANDOM_SEED = 42

OUT_PRODUCTS = "data/products_enriched_simple.csv"
OUT_TAXONOMY = "data/urls_taxonomy_simple.csv"


# =========================
# Step 0: safe sleep
# =========================
def polite_sleep(mult=1.0):
    time.sleep(mult * (DELAY_SECONDS + random.random() * JITTER_SECONDS))


# =========================
# Step 1: fetch with safe retries
# =========================
def fetch(session, url):
    """
    Fetch a URL safely.
    - Do NOT retry 404 (page doesn't exist)
    - Retry 429 and 5xx (rate limiting / server errors)
    """
    backoff = 2.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)

            if r.status_code == 404:
                return None  # no page -> no retry

            if r.status_code in (429, 500, 502, 503, 504):
                print(f"[WARN] {r.status_code} {url} (attempt {attempt}) -> backoff")
                polite_sleep(mult=backoff)
                backoff *= 2
                continue

            r.raise_for_status()
            polite_sleep()
            return r.content

        except Exception as e:
            print(f"[WARN] failed {url} (attempt {attempt}): {e}")
            polite_sleep(mult=backoff)
            backoff *= 2

    return None


# =========================
# Step 2: parse sitemap -> get product URLs
# =========================
def classify_url(url):
    if "/marketplace/pp/" in url:
        return "product"
    if "/marketplace/seller-profile/" in url:
        return "seller"
    if "/marketplace/b/" in url:
        return "category"
    return "other"


def extract_slug(url, type_):
    if type_ == "product":
        m = re.search(r"/marketplace/pp/([^/?]+)", url)
    elif type_ == "seller":
        m = re.search(r"/marketplace/seller-profile/([^/?]+)", url)
    elif type_ == "category":
        m = re.search(r"/marketplace/b/([^/?]+)", url)
    else:
        return None
    return m.group(1) if m else None


def get_product_urls_from_sitemap(session):
    print("[INFO] Fetching sitemap")
    content = fetch(session, SITEMAP_URL)
    if not content:
        raise RuntimeError("Could not fetch sitemap")

    xml = etree.parse(BytesIO(content))
    urls = [el.text for el in xml.findall(".//{*}loc") if el.text]
    print(f"[INFO] Total URLs found in sitemap: {len(urls)}")

    df = pd.DataFrame({"url": urls})
    df["type"] = df["url"].apply(classify_url)
    df["slug"] = df.apply(lambda row: extract_slug(row["url"], row["type"]), axis=1)

    df = df[df["slug"].notnull()].copy()
    df.to_csv(OUT_TAXONOMY, index=False)
    print(f"[INFO] Saved taxonomy -> {OUT_TAXONOMY}")

    product_urls = df[df["type"] == "product"]["url"].drop_duplicates().tolist()
    print(f"[INFO] Product URLs found: {len(product_urls)}")
    return product_urls


# =========================
# Step 3: extract fields from product page
# =========================
def prodview_id_from_url(url):
    # last path element is typically prodview-xxxx
    path = urlparse(url).path.strip("/")
    last = path.split("/")[-1]
    return last if last.startswith("prodview-") else None


def extract_product_name(tree):
    title = tree.xpath("//title/text()")
    if not title:
        return None
    t = title[0].strip()
    if t.lower().startswith("aws marketplace:"):
        return t.split(":", 1)[1].strip()
    return t


def extract_seller_name(tree):
    sellers = tree.xpath("//a[contains(@href,'/marketplace/seller-profile')]/text()")
    sellers = [s.strip() for s in sellers if s and s.strip()]
    return sellers[0] if sellers else None


def extract_categories(tree):
    cats = tree.xpath("//a[contains(@href,'/marketplace/b/')]/text()")
    cats = [c.strip() for c in cats if c and c.strip()]
    primary = cats[0] if cats else None
    allcats = "|".join(cats) if cats else None
    return primary, allcats


def detect_delivery_method(page_text):
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
# Step 4: pricing (simple but useful)
# =========================
def classify_pricing(page_text):
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
    if "contact seller" in t or ("contact" in t and "pricing" in t):
        return "contact_seller"
    return "unknown"


def extract_pricing(page_text):
    pricing_type = classify_pricing(page_text)

    # contract terms like "12-month contract"
    terms = sorted(set(re.findall(r"\b(\d+)\s*-\s*month contract\b", page_text, flags=re.IGNORECASE)))
    if not terms:
        terms = sorted(set(re.findall(r"\b(\d+)\s*month contract\b", page_text, flags=re.IGNORECASE)))
    contract_terms = ",".join([f"{x}-month" for x in terms]) if terms else None

    # prices like $1,000.00
    prices = re.findall(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)", page_text)
    vals = []
    for p in prices:
        try:
            vals.append(float(p.replace(",", "")))
        except:
            pass

    price_visible = 1 if vals else 0
    price_min = min(vals) if vals else None
    price_max = max(vals) if vals else None

    return {
        "pricing_type": pricing_type,
        "contract_terms": contract_terms,
        "price_visible": price_visible,
        "price_min_usd": price_min,
        "price_max_usd": price_max,
    }


# =========================
# Step 5: reviews (from reviews-list page)
# =========================
def extract_reviews_from_reviews_page(session, prodview_id):
    reviews_url = f"https://aws.amazon.com/marketplace/reviews/reviews-list/{prodview_id}"
    content = fetch(session, reviews_url)
    if not content:
        # 404 or hard fail -> treat as no reviews page
        return {
            "reviews_page_exists": 0,
            "reviews_supported": 0,
            "aws_reviews_count": None,
            "external_reviews_count": None,
            "avg_rating": None,
            "ratings_count": None,
        }

    tree = html.fromstring(content)
    text = re.sub(r"\s+", " ", tree.text_content()).strip()
    low = text.lower()

    if "reviews are not supported" in low:
        return {
            "reviews_page_exists": 1,
            "reviews_supported": 0,
            "aws_reviews_count": None,
            "external_reviews_count": None,
            "avg_rating": None,
            "ratings_count": None,
        }

    def to_int(x):
        try:
            return int(x.replace(",", ""))
        except:
            return None

    aws_reviews = None
    m = re.search(r"(\d[\d,]*)\s+AWS reviews\b", text, flags=re.IGNORECASE)
    if m:
        aws_reviews = to_int(m.group(1))

    external_reviews = None
    m = re.search(r"(\d[\d,]*)\s+external reviews\b", text, flags=re.IGNORECASE)
    if m:
        external_reviews = to_int(m.group(1))

    ratings_count = None
    m = re.search(r"(\d[\d,]*)\s+ratings\b", text, flags=re.IGNORECASE)
    if m:
        ratings_count = to_int(m.group(1))

    avg_rating = None
    m = re.search(r"\b([0-5]\.\d)\s+out of 5\b", text, flags=re.IGNORECASE)
    if m:
        try:
            avg_rating = float(m.group(1))
        except:
            pass

    return {
        "reviews_page_exists": 1,
        "reviews_supported": 1,
        "aws_reviews_count": aws_reviews,
        "external_reviews_count": external_reviews,
        "avg_rating": avg_rating,
        "ratings_count": ratings_count,
    }


# =========================
# MAIN PIPELINE
# =========================
def main():
    random.seed(RANDOM_SEED)

    with requests.Session() as session:
        # 1) Get product urls
        product_urls = get_product_urls_from_sitemap(session)

        # 2) Sample
        if SAMPLE_PRODUCTS_TOTAL is not None:
            product_urls = random.sample(product_urls, k=min(SAMPLE_PRODUCTS_TOTAL, len(product_urls)))
            print(f"[INFO] Sampling {len(product_urls)} products")

        rows = []
        for i, url in enumerate(product_urls, 1):
            print(f"\n[{i}/{len(product_urls)}] {url}")

            # Fetch product page
            content = fetch(session, url)
            if not content:
                print("[SKIP] Could not fetch product page")
                continue

            tree = html.fromstring(content)
            page_text = re.sub(r"\s+", " ", tree.text_content()).strip()

            pid = prodview_id_from_url(url)
            product_name = extract_product_name(tree)
            seller_name = extract_seller_name(tree)
            category_primary, categories_all = extract_categories(tree)
            delivery_method = detect_delivery_method(page_text)

            pricing = extract_pricing(page_text)

            # Fetch reviews page (safe: 404 -> no retries)
            reviews = extract_reviews_from_reviews_page(session, pid) if pid else {
                "reviews_page_exists": 0,
                "reviews_supported": 0,
                "aws_reviews_count": None,
                "external_reviews_count": None,
                "avg_rating": None,
                "ratings_count": None,
            }

            row = {
                "url": url,
                "prodview_id": pid,
                "product_name": product_name,
                "seller_name": seller_name,
                "category_primary": category_primary,
                "categories_all": categories_all,
                "delivery_method": delivery_method,
                **pricing,
                **reviews,
            }
            print(f"[OK] {pid} | {product_name} | pricing={pricing['pricing_type']} | reviews_page={reviews['reviews_page_exists']}")
            rows.append(row)

        df = pd.DataFrame(rows)
        df.to_csv(OUT_PRODUCTS, index=False)
        print(f"\n[DONE] Saved -> {OUT_PRODUCTS}")


if __name__ == "__main__":
    main()
