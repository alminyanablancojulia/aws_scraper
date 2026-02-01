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


# ---------------- Configuration ----------------
SITEMAP_URL = "https://aws.amazon.com/marketplace/sitemap.xml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; thesis-bot/1.0; +https://example.com)",
    "Accept-Language": "en-US,en;q=0.9",
}

DELAY_SECONDS = 1.5          # base delay
JITTER_SECONDS = 1.0         # random extra delay
MAX_RETRIES = 4
TIMEOUT = 25

SAMPLE_PRODUCTS_TOTAL = 200  # keep 200-500 while testing
RANDOM_SEED = 42

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

CACHE_DIR = OUT_DIR / "cache_html"
CACHE_DIR.mkdir(exist_ok=True)

TAXONOMY_FILE = OUT_DIR / "urls_taxonomy.csv"
PRODUCTS_FILE = OUT_DIR / "products_enriched_plus.csv"
# ------------------------------------------------


# ---------------- Utilities ----------------

def polite_sleep(multiplier: float = 1.0):
    time.sleep(multiplier * (DELAY_SECONDS + random.random() * JITTER_SECONDS))


def prodview_id_from_url(url: str) -> str | None:
    """
    Extract prodview-... from /marketplace/pp/prodview-xxxxx
    """
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if not parts:
        return None
    last = parts[-1]
    if last.startswith("prodview-"):
        return last
    return None


def cache_path(kind: str, url: str) -> Path:
    """
    Cache by prodview id if available; else hash of url.
    """
    pid = prodview_id_from_url(url)
    if pid:
        name = pid
    else:
        name = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{kind}__{name}.html"


# ---------------- Fetch helpers ----------------

def fetch_url(session: requests.Session, url: str, kind: str, use_cache=True) -> bytes | None:
    """
    Safe fetch:
    - disk cache (so debugging doesn't re-hit AWS)
    - exponential backoff for 429 / 503
    - polite delays
    """
    cpath = cache_path(kind, url)
    if use_cache and cpath.exists():
        return cpath.read_bytes()

    backoff = 2.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT)
            status = r.status_code

            # Rate limiting / transient issues
            if status in (429, 503, 502, 500):
                print(f"[WARN] {status} on {url} (attempt {attempt}/{MAX_RETRIES}) -> backing off")
                polite_sleep(multiplier=backoff)
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
            polite_sleep(multiplier=backoff)
            backoff *= 2

    print(f"[ERROR] failed to fetch after retries: {url}")
    return None


# ---------------- Sitemap parsing ----------------

def parse_url_path(url: str):
    path = urlparse(url).path.strip("/")
    return path.split("/") if path else []


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
        parts = parse_url_path(url)
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


# ---------------- Product parsing ----------------

def extract_product_name(tree: html.HtmlElement) -> str | None:
    title = tree.xpath("//title/text()")
    if not title:
        return None
    t = title[0].strip()
    if t.lower().startswith("aws marketplace:"):
        return t.split(":", 1)[1].strip()
    return t


def first_text(tree: html.HtmlElement, xpath: str) -> str | None:
    vals = tree.xpath(xpath)
    if not vals:
        return None
    # vals could be elements or strings
    if hasattr(vals[0], "text_content"):
        txt = vals[0].text_content().strip()
    else:
        txt = str(vals[0]).strip()
    return txt or None


def normalize_int(s: str) -> int | None:
    if s is None:
        return None
    s = s.strip().replace(",", "")
    return int(s) if s.isdigit() else None


def parse_reviews_from_text(page_text: str) -> dict:
    """
    Extract:
    - avg_rating (float)
    - ratings_count (int) e.g. "968 ratings"
    - aws_reviews_count (int) e.g. "3 AWS reviews"
    - external_reviews_count (int) e.g. "965 external reviews"
    """
    out = {
        "avg_rating": None,
        "ratings_count": None,
        "aws_reviews_count": None,
        "external_reviews_count": None,
        "reviews_available": None,  # 1 if any of the fields found
    }

    # counts
    m = re.search(r"(\d[\d,]*)\s+ratings\b", page_text, flags=re.IGNORECASE)
    if m:
        out["ratings_count"] = normalize_int(m.group(1))

    m = re.search(r"(\d[\d,]*)\s+AWS reviews\b", page_text, flags=re.IGNORECASE)
    if m:
        out["aws_reviews_count"] = normalize_int(m.group(1))

    m = re.search(r"(\d[\d,]*)\s+external reviews\b", page_text, flags=re.IGNORECASE)
    if m:
        out["external_reviews_count"] = normalize_int(m.group(1))

    # avg rating: try to find it near "Ratings and reviews" section
    # Approach: look for "Ratings and reviews" then scan next ~500 chars for a X.Y pattern
    idx = page_text.lower().find("ratings and reviews")
    if idx != -1:
        window = page_text[idx:idx + 800]
        m = re.search(r"\b([0-5]\.\d)\b", window)
        if m:
            out["avg_rating"] = float(m.group(1))

    # fallback: some pages show rating at top like "4.3 (2)"
    if out["avg_rating"] is None:
        m = re.search(r"\b([0-5]\.\d)\b\s*\n\s*\(?\d[\d,]*\)?", page_text)
        if m:
            try:
                out["avg_rating"] = float(m.group(1))
            except:
                pass

    found_any = any(out[k] is not None for k in ["avg_rating", "ratings_count", "aws_reviews_count", "external_reviews_count"])
    out["reviews_available"] = 1 if found_any else 0
    return out


def classify_pricing(page_text: str) -> str:
    """
    Classify pricing type using robust signals.
    Returns one of:
    free, free_trial, hourly, monthly, annual, contract, usage_based, byol, contact_seller, unknown
    """
    t = page_text.lower()

    if "free trial" in t:
        return "free_trial"
    # sometimes "Free" products
    if re.search(r"\bfree\b", t) and ("$" not in t) and ("cost/" not in t):
        # weak signal, but useful
        return "free"

    if "bring your own license" in t or "byol" in t:
        return "byol"

    if "usage-based" in t or "usage based" in t:
        return "usage_based"

    # hourly / monthly / annual signals
    if "cost/hour" in t or "hourly" in t:
        return "hourly"
    if "cost/month" in t:
        return "monthly"
    if "cost/12 months" in t or "cost/12-month" in t or "12-month contract" in t:
        return "contract"
    if "annual" in t:
        return "annual"

    if "contact" in t and "pricing" in t:
        return "contact_seller"

    # many SaaS listings use contract language
    if "pricing is based on the duration and terms of your contract" in t:
        return "contract"

    return "unknown"


def parse_pricing_details(page_text: str) -> dict:
    """
    Extract:
    - pricing_type
    - contract_terms: list like ["1-month", "12-month", ...] (stringified)
    - price_min_usd, price_max_usd (when visible)
    - price_visible (0/1)
    """
    out = {
        "pricing_type": classify_pricing(page_text),
        "contract_terms": None,
        "price_min_usd": None,
        "price_max_usd": None,
        "price_visible": 0
    }

    # contract terms shown as bullet points like "12-month contract"
    terms = sorted(set(re.findall(r"\b(\d+)-month contract\b", page_text, flags=re.IGNORECASE)))
    if terms:
        out["contract_terms"] = ",".join([f"{t}-month" for t in terms])

    # extract USD prices (table often includes "$170,000.00")
    prices = re.findall(r"\$\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)", page_text)
    if prices:
        vals = []
        for p in prices:
            try:
                vals.append(float(p.replace(",", "")))
            except:
                pass
        if vals:
            out["price_min_usd"] = min(vals)
            out["price_max_usd"] = max(vals)
            out["price_visible"] = 1

    return out


def fetch_product_info(session: requests.Session, url: str, use_cache=True) -> dict | None:
    content = fetch_url(session, url, kind="product", use_cache=use_cache)
    if not content:
        return None

    tree = html.fromstring(content)
    page_text = tree.text_content()
    page_text = re.sub(r"\s+", " ", page_text).strip()  # normalize whitespace

    product_name = extract_product_name(tree)
    seller_name = first_text(tree, "//a[contains(@href,'/marketplace/seller-profile')]/text()")

    # categories: there may be several; collect them
    categories = tree.xpath("//a[contains(@href,'/marketplace/b/')]/text()")
    categories = [c.strip() for c in categories if c and c.strip()]
    category_primary = categories[0] if categories else None
    categories_all = "|".join(categories) if categories else None

    # delivery method text usually appears in Details
    delivery_method = None
    # Try to detect common ones in the text content
    for dm in ["Software as a Service (SaaS)", "Amazon Machine Image (AMI)", "Container", "Data", "Professional Services"]:
        if dm.lower() in page_text.lower():
            delivery_method = dm
            break

    # reviews + pricing
    review_fields = parse_reviews_from_text(page_text)
    pricing_fields = parse_pricing_details(page_text)

    pid = prodview_id_from_url(url)

    print("[PRODUCT]", product_name, "| seller:", seller_name, "| pricing:", pricing_fields["pricing_type"],
          "| ratings:", review_fields["ratings_count"], "| AWS reviews:", review_fields["aws_reviews_count"])

    return {
        "url": url,
        "prodview_id": pid,
        "product_name": product_name,
        "seller_name": seller_name,
        "category_primary": category_primary,
        "categories_all": categories_all,
        "delivery_method": delivery_method,
        **pricing_fields,
        **review_fields,
    }


# ---------------- Main pipeline ----------------

def main():
    with requests.Session() as session:
        # 1) Parse sitemap → taxonomy
        taxonomy_df = parse_sitemap(session)
        taxonomy_df.to_csv(TAXONOMY_FILE, index=False)
        print(f"[INFO] taxonomy saved to {TAXONOMY_FILE}")

        # 2) Product URLs only
        product_urls = taxonomy_df[taxonomy_df["section"] == "pp"]["url"].unique()
        print(f"[INFO] {len(product_urls)} product URLs found")

        # 3) Deterministic sampling
        rng = random.Random(RANDOM_SEED)
        sampled_urls = rng.sample(list(product_urls), k=min(SAMPLE_PRODUCTS_TOTAL, len(product_urls)))
        print(f"[INFO] Sampling {len(sampled_urls)} products")

        # 4) Enrich products
        enriched = []
        for i, url in enumerate(sampled_urls, 1):
            print(f"\n[{i}/{len(sampled_urls)}] fetching product")
            info = fetch_product_info(session, url, use_cache=True)
            if info and info.get("product_name"):
                enriched.append(info)

        products_df = pd.DataFrame(enriched)
        products_df.to_csv(PRODUCTS_FILE, index=False)
        print(f"\n[INFO] enriched products saved to {PRODUCTS_FILE}")

        # 5) Quick sanity checks
        print("\n[INSIGHT] Pricing types:")
        print(products_df["pricing_type"].value_counts(dropna=False).head(15))

        print("\n[INSIGHT] Top sellers:")
        print(products_df["seller_name"].value_counts(dropna=False).head(10))

        print("\n[INSIGHT] Reviews availability:")
        print(products_df["reviews_available"].value_counts(dropna=False))


if __name__ == "__main__":
    main()
