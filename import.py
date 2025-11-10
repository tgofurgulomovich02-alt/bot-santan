#!/usr/bin/env python3
import os, re, glob, math, sqlite3, hashlib
from typing import List, Dict, Tuple, Optional
import pandas as pd

# === Paths ===
BASE_DIR = os.path.dirname(__file__)
CATALOG_DIR = os.path.join(BASE_DIR, "catalog")
DB_PATH = os.path.join(BASE_DIR, "products.db")
  # your bot uses shop.db

# === DB schema expected by your bot.py ===
#   SELECT sku,title,price,category,subcategory,description,image_url,image_path,stock ...
CREATE_SQL = """
CREATE TABLE IF NOT EXISTS products (
    sku TEXT PRIMARY KEY,
    title TEXT,
    price REAL,
    category TEXT,
    subcategory TEXT,
    description TEXT,
    image_url TEXT,
    image_path TEXT,
    stock INTEGER
);
"""

# ---------- helpers ----------
def db():
    return sqlite3.connect(DB_PATH)

def ensure_schema() -> None:
    with db() as conn:
        conn.execute(CREATE_SQL)
        conn.commit()

def md5(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def clean_txt(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    return re.sub(r"\s+", " ", str(x)).strip()

CURR_RE = re.compile(r"(usd|\$|uzs|so'?m|сум)", re.IGNORECASE)

def parse_price(v) -> Optional[float]:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    s = str(v).strip()
    if not s:
        return None
    s = CURR_RE.sub("", s)           # strip currency text
    s = s.replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(",", "")       # 1,234.56 -> 1234.56
    elif "," in s and "." not in s:
        s = s.replace(",", ".")      # 1234,56 -> 1234.56
    try:
        return float(s)
    except ValueError:
        return None

# header synonyms
ALIASES = {
    "category": {"категория", "Категория", "category", "kategoriya"},
    "title": {"наименование товара", "Наименование товара", "наименование", "Наименование", "name", "product"},
    "price": {"цена", "Цена", "стоимость", "Цена розница без скидки", "цена розница без скидки", "price", "narx"},
}

def norm(h: str) -> str:
    return re.sub(r"\s+", " ", str(h)).strip().lower()

def map_columns(cols: List[str]) -> Dict[str, Optional[str]]:
    norm_map = {norm(c): c for c in cols}
    out = {"category": None, "title": None, "price": None}
    for key, aliases in ALIASES.items():
        for a in aliases:
            if norm(a) in norm_map:
                out[key] = norm_map[norm(a)]
                break
        if out[key] is None:
            # fuzzy
            for k, orig in norm_map.items():
                if key == "category" and ("категор" in k or "category" in k or "kategoriya" in k):
                    out[key] = orig; break
                if key == "title" and ("наимен" in k or "name" in k or "товар" in k):
                    out[key] = orig; break
                if key == "price" and ("цена" in k or "price" in k or "стоим" in k):
                    out[key] = orig; break
    return out

def import_dataframe(df: pd.DataFrame, src_file: str, sheet: str,
                     default_currency: str, usd_rate: float) -> Tuple[int, int, int]:
    """Returns (imported_ok, skipped_missing_cols, skipped_bad_rows)"""
    if df is None or df.empty:
        return (0, 0, 0)

    # drop unnamed filler columns
    df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed")]]

    cm = map_columns(list(df.columns))
    cat_col, title_col, price_col = cm["category"], cm["title"], cm["price"]
    if not (cat_col and title_col and price_col):
        print(f"  [skip] {os.path.basename(src_file)} / {sheet}: required columns not found")
        print(f"        found: {list(df.columns)}")
        return (0, 1, 0)

    tmp = df[[cat_col, title_col, price_col]].copy()
    tmp.columns = ["category", "title", "price"]
    tmp["category"] = tmp["category"].map(clean_txt)
    tmp["title"]    = tmp["title"].map(clean_txt)
    tmp["price"]    = tmp["price"].map(parse_price)

    # Convert USD -> UZS if requested
    if default_currency.upper() == "USD":
        print(f"    [info] Converting USD -> UZS @ {usd_rate:.2f}")
        tmp["price"] = tmp["price"].apply(lambda x: x * usd_rate if x is not None else None)

    before = len(tmp)
    tmp = tmp.dropna(subset=["category", "title", "price"])
    bad_rows = before - len(tmp)

    # Upsert to DB
    with db() as conn:
        cur = conn.cursor()
        ok = 0
        for _, r in tmp.iterrows():
            category = r["category"]
            title    = r["title"]
            price    = float(r["price"])
            # build a stable SKU if not provided in file: md5(category|title)
            sku = md5(f"{category}|{title}")

            cur.execute("""
                INSERT INTO products (sku, title, price, category, subcategory, description, image_url, image_path, stock)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(sku) DO UPDATE SET
                    title=excluded.title,
                    price=excluded.price,
                    category=excluded.category
            """, (sku, title, price, category, "", "", "", "", 1))
            ok += 1
        conn.commit()
    return (ok, 0, bad_rows)

def import_file(path: str, default_currency: str, usd_rate: float) -> Tuple[int, int, int]:
    print(f"\n==> Importing: {os.path.basename(path)}")
    try:
        xls = pd.ExcelFile(path)
    except Exception as e:
        print(f"  [!] cannot open: {e}")
        return (0,0,0)

    tot_ok = tot_miss = tot_bad = 0
    for sheet in xls.sheet_names:
        try:
            df = pd.read_excel(path, sheet_name=sheet)
        except Exception as e:
            print(f"  [!] cannot read sheet '{sheet}': {e}")
            continue
        ok, miss, bad = import_dataframe(df, path, sheet, default_currency, usd_rate)
        print(f"  - {sheet}: imported={ok}, missing_cols={miss}, bad_rows={bad}")
        tot_ok += ok; tot_miss += miss; tot_bad += bad
    return (tot_ok, tot_miss, tot_bad)

def import_all() -> None:
    ensure_schema()

    default_currency = os.getenv("DEFAULT_PRICE_CURRENCY", "UZS")
    usd_rate = float(os.getenv("USD_RATE", "12700"))

    files = sorted(glob.glob(os.path.join(CATALOG_DIR, "*.xlsx")) +
                   glob.glob(os.path.join(CATALOG_DIR, "*.xls")))
    if not files:
        print(f"No Excel files in {CATALOG_DIR}")
        return

    g_ok = g_miss = g_bad = 0
    for fp in files:
        ok, miss, bad = import_file(fp, default_currency, usd_rate)
        g_ok += ok; g_miss += miss; g_bad += bad

    print("\n==== SUMMARY ====")
    print(f"Imported rows:          {g_ok}")
    print(f"Sheets missing columns: {g_miss}")
    print(f"Rows skipped (bad):     {g_bad}")

if __name__ == "__main__":
    import_all()
