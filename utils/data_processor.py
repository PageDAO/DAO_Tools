import pandas as pd
import json
import os
from datetime import datetime
import requests

class DataProcessor:
	def __init__(self, core_team_addresses=None, token_data=None):
		self.core_team_addresses = set(core_team_addresses or [])
		self.token_data = token_data or {}
		self.price_cache = {}

	def _load_pricing_data(self):
		"""Load combined pricing JSON files from attached_assets or pagedata."""
		search_paths = [
			"attached_assets/combined_daily_prices_1756000184191.json",
			"attached_assets/osmo_secret_daily_prices.json",
			"attached_assets/combined_daily_prices.json",
			"pagedata/combined_daily_prices.json",
			"combined_daily_prices.json",
		]

		combined = {}
		for p in search_paths:
			if os.path.exists(p):
				try:
					with open(p, 'r', encoding='utf-8') as f:
						data = json.load(f)
						if isinstance(data, dict):
							combined.update(data)
				except Exception:
					continue
		return combined

	def _convert_token_amount(self, amount, decimals):
		try:
			if pd.isna(amount):
				return 0
		except Exception:
			pass
		try:
			return float(amount) / (10 ** int(decimals))
		except Exception:
			try:
				return float(amount)
			except Exception:
				return 0

	def _lookup_usd_price(self, symbol, date):
		# date is a string like '2023-01-02' or datetime
		if not symbol:
			return None
		if isinstance(date, datetime):
			date_key = date.strftime('%Y-%m-%d')
		else:
			try:
				date_key = pd.to_datetime(date).strftime('%Y-%m-%d')
			except Exception:
				date_key = str(date)

		# Use the cached price if available
		if (symbol, date_key) in self.price_cache:
			return self.price_cache[(symbol, date_key)]

		# Try to read from combined pricing JSONs
		combined = self._load_pricing_data()
		if symbol in combined:
			symbol_prices = combined.get(symbol, {})
			if date_key in symbol_prices:
				price = symbol_prices.get(date_key)
				try:
					price_f = float(price)
					self.price_cache[(symbol, date_key)] = price_f
					return price_f
				except Exception:
					pass

		# Fallback: try Coingecko simple API for a recent price
		# Note: this is a best effort; not all tokens are on coingecko under the same symbol.
		try:
			url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol}&vs_currencies=usd"
			r = requests.get(url, timeout=10)
			r.raise_for_status()
			data = r.json()
			if symbol in data and 'usd' in data[symbol]:
				price = float(data[symbol]['usd'])
				self.price_cache[(symbol, date_key)] = price
				return price
		except Exception:
			pass

		# Not found
		return None

	def _extract_proposal_date(self, proposal):
		# Try several fields for a proposal date
		for key in ['final_queued_at', 'submission_time', 'created_at', 'start_time', 'timestamp']:
			if key in proposal and proposal.get(key):
				try:
					return pd.to_datetime(proposal.get(key))
				except Exception:
					continue
		# Try nested
		if 'metadata' in proposal and isinstance(proposal['metadata'], dict):
			if 'created_at' in proposal['metadata']:
				try:
					return pd.to_datetime(proposal['metadata']['created_at'])
				except Exception:
					pass
		return None

	def process_all_proposals(self, proposals_by_subdao):
		"""Takes a dict: { 'SubDAO Name': {'address': addr, 'proposals': [...] } }
		   Returns a pandas DataFrame of normalized transactions with USD conversion.
		"""
		rows = []
		combined_prices = self._load_pricing_data()

		for subunit_name, payload in proposals_by_subdao.items():
			proposals = payload.get('proposals') if isinstance(payload, dict) else []
			for p in proposals:
				p_date = self._extract_proposal_date(p)
				proposal_id = p.get('proposal_id') or p.get('id') or p.get('proposal_id')
				title = p.get('title') or p.get('metadata', {}).get('title') if isinstance(p.get('metadata'), dict) else p.get('title')

				# messages could be in different shapes
				msgs = []
				if 'messages' in p and isinstance(p['messages'], list):
					msgs = p['messages']
				elif 'msgs' in p and isinstance(p['msgs'], list):
					msgs = p['msgs']
				else:
					# try to find from 'actions' or 'msgs'
					if 'actions' in p and isinstance(p['actions'], list):
						msgs = p['actions']

				# When messages include funds or multiple recipients, expand per recipient
				for m in msgs:
					# Filter out wasm_execute_funds
					mtype = m.get('type') or m.get('@type') or m.get('msg_type') or m.get('action')
					mtype_str = str(mtype) if mtype else ''
					if 'wasm_execute_funds' in mtype_str:
						continue

					# Normalized recipient and amounts
					recipients = []
					amounts = []

					# Messages may have 'funds', 'amount', 'coins', 'transfers', or nested messages
					if isinstance(m, dict):
						# common key names
						if 'funds' in m and isinstance(m['funds'], list):
							for f in m['funds']:
								recipients.append(m.get('to_address') or m.get('recipient') or f.get('recipient') or m.get('address'))
								amounts.append((f.get('amount') or f.get('value') or f.get('coin', {}).get('amount'), f.get('denom') or f.get('token') or f.get('coin', {}).get('denom')))
						elif 'amount' in m and isinstance(m['amount'], list):
							for f in m['amount']:
								recipients.append(m.get('recipient') or m.get('to_address') or m.get('address'))
								amounts.append((f.get('amount') or f.get('value'), f.get('denom') or f.get('token')))
						elif 'coins' in m and isinstance(m['coins'], list):
							for f in m['coins']:
								recipients.append(m.get('recipient') or m.get('to_address') or m.get('address'))
								amounts.append((f.get('amount') or f.get('value'), f.get('denom') or f.get('token')))
						elif 'transfers' in m and isinstance(m['transfers'], list):
							for t in m['transfers']:
								recipients.append(t.get('to') or t.get('recipient') or t.get('address'))
								amounts.append((t.get('amount'), t.get('denom') or t.get('token')))
						else:
							# try to detect single recipient + amount fields
							recipient = m.get('recipient') or m.get('to') or m.get('to_address') or m.get('address')
							if 'amount' in m and (isinstance(m['amount'], (str, int, float)) or isinstance(m['amount'], dict)):
								amt = m['amount']
								if isinstance(amt, dict):
									# try to pull denom/amount
									a = amt.get('amount') or amt.get('value')
									d = amt.get('denom') or amt.get('denomination')
									amounts.append((a, d))
								else:
									amounts.append((amt, m.get('denom') or m.get('token')))
									recipients.append(recipient)
							else:
								# Nothing obvious; create a placeholder entry so it can be inspected
								recipients.append(recipient)
								amounts.append((None, None))

					# If there were no recipients discovered, skip
					if not recipients:
						continue

					# Now expand recipients/amounts coherently
					for i, rec in enumerate(recipients):
						amt_pair = amounts[i] if i < len(amounts) else (None, None)
						raw_amount, denom = amt_pair

						decimals = 0
						symbol = denom
						# Map denom to symbol if token metadata available
						if denom and self.token_data and denom in self.token_data:
							token_meta = self.token_data.get(denom, {})
							symbol = token_meta.get('symbol', denom)
							decimals = token_meta.get('decimals', 0)

						token_amount = self._convert_token_amount(raw_amount, decimals)

						usd_price = None
						if symbol:
							usd_price = self._lookup_usd_price(symbol, p_date or datetime.now())

						usd_value = None
						if usd_price is not None and token_amount is not None:
							try:
								usd_value = float(token_amount) * float(usd_price)
							except Exception:
								usd_value = None

						# Determine recipient type
						recipient_type = 'Unknown'
						if rec in self.core_team_addresses:
							recipient_type = 'Core Team'
						elif isinstance(rec, str) and rec.startswith('osmo'):
							recipient_type = 'On-Chain Address'

						rows.append({
							'Proposal Date': p_date,
							'Proposal ID': proposal_id,
							'Proposal Title': title,
							'Sub-unit': subunit_name,
							'Org Unit': subunit_name,
							'Recipient': rec,
							'Recipient Type': recipient_type,
							'Message Type': mtype_str,
							'Token Amount': token_amount,
							'Token Denom': denom,
							'Token Symbol': symbol,
							'USD Price': usd_price,
							'USD Value': usd_value,
						})

		# Build DataFrame
		if not rows:
			return pd.DataFrame()

		df = pd.DataFrame(rows)

		# Normalize columns and types
		# Convert Proposal Date to datetime
		if 'Proposal Date' in df.columns:
			try:
				df['Proposal Date'] = pd.to_datetime(df['Proposal Date'])
			except Exception:
				pass

		# Remove duplicates
		if not df.empty:
			df = df.drop_duplicates()

		# Replace NaN USD value with 0 or None depending on needs; keep None so filter can exclude
		try:
			df['USD Value'] = df['USD Value'].where(df['USD Value'].notna(), None)
		except Exception:
			pass

		# Add category heuristics
		df['Transaction Category'] = df['Message Type'].apply(lambda x: 'Staking' if 'delegate' in str(x).lower() or 'undelegate' in str(x).lower() else 'Payment')

		# Ensure 'Main DAO' key is preserved in the Sub-unit column if present in the payload
		df['Sub-unit'] = df['Sub-unit'].fillna('Main DAO')

		# Final clean: filter out wasm_execute_funds message types (double-check)
		df = df[~df['Message Type'].astype(str).str.contains('wasm_execute_funds', na=False)]

		return df