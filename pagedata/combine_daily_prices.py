import csv
import json

PAGE_CSV = "page_osmo_daily_prices.csv"
OSMO_SECRET_CSV = "osmo_secret_daily_prices.csv"
OUTPUT_JSON = "combined_daily_prices.json"

result = []

# Read PAGE prices
with open(PAGE_CSV, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        result.append({
            "date": row["date"],
            "token": "PAGE",
            "price": float(row["price"])
        })

# Read OSMO/SECRET prices
with open(OSMO_SECRET_CSV, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Try to find the right column for symbol and price
        symbol = row.get("symbol") or row.get("token") or row.get("Symbol")
        price = row.get("AVG_DAILY_PRICE") or row.get("price")
        if symbol and price:
            result.append({
                "date": row["date"],
                "token": symbol,
                "price": float(price)
            })

# Output as JSON
with open(OUTPUT_JSON, "w") as f:
    json.dump(result, f, indent=2)

print(f"Combined data written to {OUTPUT_JSON}")
