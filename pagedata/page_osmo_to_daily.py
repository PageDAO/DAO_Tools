import json
import csv
from datetime import datetime
from collections import defaultdict

INPUT_FILE = "page_osmo_transactions.json"
OUTPUT_FILE = "page_osmo_daily_prices.csv"
TOKEN = "PAGE.grv"

def main():
    with open(INPUT_FILE, "r") as f:
        transactions = json.load(f)

    # Group prices by date, handling swap direction
    prices_by_date = defaultdict(list)
    for tx in transactions:
        ts = tx.get("timestamp")
        page_amount = tx.get("primaryTokenAmount")
        uosmo_amount = tx.get("secondaryTokenAmount")
        if ts is not None and page_amount and uosmo_amount and page_amount != 0:
            date_str = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            price = uosmo_amount / page_amount
            prices_by_date[date_str].append(price)

    # Write daily average prices to CSV
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "token", "price"])
        for date in sorted(prices_by_date.keys()):
            daily_prices = prices_by_date[date]
            avg_price = sum(daily_prices) / len(daily_prices)
            writer.writerow([date, TOKEN, avg_price])
            print(date, TOKEN, avg_price)

if __name__ == "__main__":
    main()
