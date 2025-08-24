
import requests
import csv
from datetime import datetime, timedelta

TFM_URL = "https://analytics.api.tfm.com/graphql2"
PAGE_DENOM = "ibc/23A62409E4AD8133116C249B1FA38EED30E500A115D7B153109462CD82C1CD99"


def fetch_all_transactions():
                import json
                headers = {
                                "accept": "application/json",
                                "content-type": "application/json",
                                "origin": "https://app.tfm.com",
                                "referer": "https://app.tfm.com/",
                                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
                }
                query = """
                query getTransactions ($chain: [String!]!, $tokens: PairTokensFilter, $action: TransactionAction, $sort: TransactionSorting, $limit: Int, $skip: Int, $asc: Boolean!) {
                        transaction (
                                chain: $chain
                                tokens: $tokens
                                action: $action
                                sort: $sort
                                limit: $limit
                                skip: $skip
                                asc: $asc
                        ) {
                                items {
                                        volume
                                        txHash
                                        timestamp
                                        swapType
                                        sender
                                        secondaryVolume
                                        secondaryTokenAmount
                                        secondaryTokenAddress
                                        secondaryPrice
                                        primaryVolume
                                        primaryTokenAmount
                                        primaryTokenAddress
                                        primaryPrice
                                }
                                pageInfo {
                                        hasNextPage
                                        skip
                                        limit
                                }
                        }
                }
                """
                variables = {
                                "chain": ["osmosis"],
                                "tokens": {"token0": PAGE_DENOM, "token1": "uosmo"},
                                "action": "SWAP",
                                "sort": "TIMESTAMP",
                                "limit": 100,
                                "skip": 0,
                                "asc": False
                }
                all_items = []
                page = 0
                while True:
                                payload = {"query": query, "variables": variables}
                                response = requests.post(TFM_URL, headers=headers, json=payload)
                                data = response.json()
                                print(f"Page {page} response: {data}")
                                if data.get("data") and data["data"].get("transaction"):
                                                items = data["data"]["transaction"]["items"]
                                                all_items.extend(items)
                                                pageInfo = data["data"]["transaction"]["pageInfo"]
                                                if pageInfo.get("hasNextPage"):
                                                                variables["skip"] += variables["limit"]
                                                                page += 1
                                                else:
                                                                break
                                else:
                                                break
                # Save all items to a JSON file
                with open("page_osmo_transactions.json", "w") as f:
                                json.dump(all_items, f, indent=2)
                print(f"Saved {len(all_items)} transactions to page_osmo_transactions.json")


if __name__ == "__main__":
        fetch_all_transactions()
