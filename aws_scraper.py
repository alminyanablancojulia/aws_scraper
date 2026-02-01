import requests
import time
import random
from lxml import html, etree
from io import BytesIO
import pandas as pd
from pathlib import Path
from urllib.parse import urlparse

# ---------------- Configuration ----------------
SITEMAP_URL = "https://aws.amazon.com/marketplace/sitemap.xml"
HEADERS = {"User-Agent": "Mozilla/5.0"}
DELAY_SECONDS = 1.5
SAMPLE_PRODUCTS_TOTAL = 2000   # keep small while testing

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

TAXONOMY_FILE = OUT_DIR / "urls_taxonomy.csv"
PRODUCTS_FILE = OUT_DIR / "products_enriched.csv"
# ------------------------------------------------


# ---------------- Fetch helpers ----------------

def fetch_url(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            time.sleep(DELAY_SECONDS + random.random())
            return r.content
        except Exception as e:
            print(f"[WARN] attempt {attempt+1} failed for {url}: {e}")
            time.sleep(DELAY_SECONDS)
    print(f"[ERROR] failed to fetch {url}")
    return None


# ---------------- Sitemap parsing ----------------

def parse_url_path(url):
    path = urlparse(url).path.strip("/")
    return path.split("/") if path else []


def parse_sitemap():
    print("[INFO] Fetching sitemap")
    content = fetch_url(SITEMAP_URL)
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

def extract_product_name(tree):
    """
    Reliable extraction from server-rendered <title data-rh="true">
    Example:
    'AWS Marketplace: Brave Search API Pro'
    """
    title = tree.xpath("//title/text()")
    if not title:
        return None

    title = title[0].strip()

    if title.lower().startswith("aws marketplace:"):
        return title.split(":", 1)[1].strip()

    return title


def fetch_product_info(url):
    content = fetch_url(url)
    if not content:
        return None

    tree = html.fromstring(content)

    product_name = extract_product_name(tree)

    seller_candidates = tree.xpath(
        "//a[contains(@href,'/marketplace/seller-profile')]/text()"
    )
    seller_name = seller_candidates[0].strip() if seller_candidates else None

    category_candidates = tree.xpath(
        "//a[contains(@href,'/marketplace/b/')]/text()"
    )
    category_name = category_candidates[0].strip() if category_candidates else None

    print(
        "[PRODUCT]",
        product_name,
        "| seller:", seller_name,
        "| category:", category_name
    )

    return {
        "url": url,
        "product_name": product_name,
        "seller_name": seller_name,
        "category_name": category_name,
    }


# ---------------- Main pipeline ----------------

def main():
    # 1. Parse sitemap → structural taxonomy
    taxonomy_df = parse_sitemap()
    taxonomy_df.to_csv(TAXONOMY_FILE, index=False)
    print(f"[INFO] taxonomy saved to {TAXONOMY_FILE}")

    # 2. Structural insights (free intelligence)
    print("\n[INSIGHT] Marketplace sections:")
    print(taxonomy_df["section"].value_counts().head(10))

    print("\n[INSIGHT] Most common level_2 topics:")
    print(taxonomy_df["level_2"].value_counts().head(15))

    # 3. Product URLs only
    product_urls = taxonomy_df[taxonomy_df["section"] == "pp"]["url"].unique()
    print(f"\n[INFO] {len(product_urls)} product URLs found")

    # 4. Deterministic sampling
    rng = random.Random(42)
    sampled_urls = rng.sample(
        list(product_urls),
        k=min(SAMPLE_PRODUCTS_TOTAL, len(product_urls))
    )

    enriched = []
    for i, url in enumerate(sampled_urls, 1):
        print(f"\n[{i}/{len(sampled_urls)}] fetching product")
        info = fetch_product_info(url)
        if info and info["product_name"]:
            enriched.append(info)

    products_df = pd.DataFrame(enriched)
    products_df.to_csv(PRODUCTS_FILE, index=False)
    print(f"\n[INFO] enriched products saved to {PRODUCTS_FILE}")

    # 5. Human-readable insights
    print("\n[INSIGHT] Top sellers in sample:")
    print(products_df["seller_name"].value_counts().head(10))

    print("\n[INSIGHT] Categories in sample:")
    print(products_df["category_name"].value_counts())

    print("\n[INFO] Sample rows:")
    print(products_df.head())


if __name__ == "__main__":
    main()
