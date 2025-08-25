import io
import pandas as pd
from reportlab.lib.pagesizes import landscape, letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib import colors

class ReportGenerator:
	def __init__(self):
		pass

	def generate_summary_stats(self, df: pd.DataFrame):
		if df is None or df.empty:
			return {
				'total_payments': 0,
				'total_amount_osmo': 0,
				'total_usd_value': 0,
				'core_team_payments': 0,
				'subunits_count': 0
			}

		total_payments = len(df)
		total_usd_value = df['USD Value'].dropna().sum() if 'USD Value' in df.columns else 0
		total_amount_osmo = df['Token Amount'].dropna().sum() if 'Token Amount' in df.columns else 0
		core_team_payments = len(df[df['Recipient Type'] == 'Core Team']) if 'Recipient Type' in df.columns else 0
		subunits_count = df['Sub-unit'].nunique() if 'Sub-unit' in df.columns else 0

		return {
			'total_payments': total_payments,
			'total_amount_osmo': total_amount_osmo,
			'total_usd_value': float(total_usd_value) if total_usd_value is not None else 0,
			'core_team_payments': core_team_payments,
			'subunits_count': subunits_count
		}

	def generate_transaction_insights(self, df: pd.DataFrame):
		if df is None or df.empty:
			return {}
		insights = {}
		# largest transaction
		if 'USD Value' in df.columns and df['USD Value'].notna().any():
			try:
				largest = df.loc[df['USD Value'].idxmax()].to_dict()
				insights['largest_transaction'] = largest
			except Exception:
				pass

		# most frequent recipient
		try:
			freq = df.groupby('Recipient').agg({'USD Value': ['sum', 'count']}).reset_index()
			freq.columns = ['Recipient', 'Total USD', 'Count']
			freq = freq.sort_values('Total USD', ascending=False)
			if not freq.empty:
				insights['most_frequent_recipient'] = freq.iloc[0].to_dict()
		except Exception:
			pass

		# core team pct
		try:
			total_usd = df['USD Value'].dropna().sum() if 'USD Value' in df.columns else 0
			core_usd = df[df['Recipient Type'] == 'Core Team']['USD Value'].dropna().sum() if 'Recipient Type' in df.columns and 'USD Value' in df.columns else 0
			insights['core_team_percentage'] = (core_usd / total_usd * 100) if total_usd and core_usd else 0
		except Exception:
			insights['core_team_percentage'] = 0

		return insights

	def generate_subunit_summary(self, df: pd.DataFrame):
		if df is None or df.empty:
			return pd.DataFrame()
		if 'USD Value' not in df.columns:
			return pd.DataFrame()
		try:
			summary = df.groupby('Sub-unit').agg({'USD Value': ['sum', 'count']}).round(2)
			summary.columns = ['Total USD', 'Transactions']
			summary = summary.reset_index().sort_values('Total USD', ascending=False)
			return summary
		except Exception:
			return pd.DataFrame()

	def generate_category_breakdown(self, df: pd.DataFrame):
		if df is None or df.empty or 'Transaction Category' not in df.columns:
			return pd.DataFrame()
		try:
			c = df.groupby('Transaction Category').agg({'USD Value': 'sum', 'Proposal ID': 'count'}).round(2)
			c.columns = ['Total USD', 'Transactions']
			c = c.reset_index().sort_values('Total USD', ascending=False)
			return c
		except Exception:
			return pd.DataFrame()

	def generate_amount_range_analysis(self, df: pd.DataFrame):
		if df is None or df.empty or 'USD Value' not in df.columns:
			return pd.DataFrame()
		try:
			bins = [0, 100, 1000, 10000, 100000, 1_000_000_000]
			labels = ['0-100', '100-1k', '1k-10k', '10k-100k', '100k+']
			df['Amount Category'] = pd.cut(df['USD Value'].fillna(0), bins=bins, labels=labels, include_lowest=True)
			a = df.groupby('Amount Category').agg({'USD Value': ['sum', 'count']}).round(2)
			a.columns = ['Total USD', 'Number of Transactions']
			a = a.reset_index()
			return a
		except Exception:
			return pd.DataFrame()

	def generate_core_team_breakdown(self, df: pd.DataFrame):
		if df is None or df.empty or 'Recipient Type' not in df.columns:
			return pd.DataFrame()
		try:
			core = df.groupby('Recipient Type').agg({'USD Value': ['sum', 'count']}).round(2)
			core.columns = ['Total USD', 'Number of Transactions']
			core = core.reset_index()
			return core
		except Exception:
			return pd.DataFrame()

	def generate_detailed_report(self, df: pd.DataFrame, include_zero_usd=False):
		if df is None or df.empty:
			return pd.DataFrame()
		cols = ['Proposal Date', 'Proposal ID', 'Proposal Title', 'Sub-unit', 'Org Unit', 'Recipient', 'Recipient Type', 'Message Type', 'Token Amount', 'Token Symbol', 'USD Value']
		for c in cols:
			if c not in df.columns:
				df[c] = None

		out = df[cols].copy()

		# Convert Proposal Date to date-only for display
		try:
			out['Proposal Date'] = pd.to_datetime(out['Proposal Date']).dt.date
		except Exception:
			pass

		if not include_zero_usd:
			out = out[(out['USD Value'].notna()) & (out['USD Value'] > 0)]

		# Remove duplicates by Proposal ID + Recipient + Token Amount
		out = out.drop_duplicates(subset=['Proposal ID', 'Recipient', 'Token Amount'])

		# Rename for user-friendly labels
		out = out.rename(columns={'Sub-unit': 'Org Unit', 'Token Symbol': 'token_symbol'})

		# Ensure column order per requirement
		final_cols = ['Proposal Date', 'Proposal ID', 'Proposal Title', 'Org Unit', 'USD Value', 'Recipient', 'Recipient Type', 'Message Type', 'Token Amount', 'token_symbol']
		out = out[final_cols]

		return out

	def export_to_pdf(self, processed_data: pd.DataFrame, detailed_df: pd.DataFrame, title: str = None, include_zero_usd=False, subdaos=None, main_dao=None, core_team=None, proposals_count=0):
		buffer = io.BytesIO()
		c = canvas.Canvas(buffer, pagesize=landscape(letter))
		width, height = landscape(letter)

		# Title
		c.setFont('Helvetica-Bold', 16)
		c.drawString(1 * inch, height - 1 * inch, title or 'DAO Accounting Report')

		# Summary lines
		c.setFont('Helvetica', 10)
		y = height - 1.5 * inch
		c.drawString(1 * inch, y, f"Sub-units: {', '.join(subdaos) if subdaos else 'N/A'}")
		y -= 0.25 * inch
		c.drawString(1 * inch, y, f"Main DAO: {main_dao or 'N/A'} | Proposals fetched: {proposals_count}")
		y -= 0.25 * inch
		c.drawString(1 * inch, y, f"Core team members: {len(core_team) if core_team else 0}")

		# Table header for transactions
		y -= 0.5 * inch
		c.setFont('Helvetica-Bold', 9)
		headers = ['Proposal Date', 'Proposal ID', 'Proposal Title', 'Org Unit', 'USD Value', 'Recipient', 'Recipient Type', 'Message Type', 'Token Amount', 'Token Symbol']
		x_positions = [1 * inch, 2.2 * inch, 3.2 * inch, 6 * inch, 7.2 * inch, 8.4 * inch, 10 * inch, 11 * inch, 12 * inch, 13 * inch]

		for h, x in zip(headers, x_positions):
			c.drawString(x, y, h)

		c.setFont('Helvetica', 8)
		y -= 0.2 * inch

		# Paginate rows
		rows_written = 0
		if detailed_df is None or detailed_df.empty:
			c.drawString(1 * inch, y, "No transaction data available")
		else:
			# Ensure columns are in expected order
			for _, row in detailed_df.iterrows():
				if y < 1 * inch:
					c.showPage()
					c.setFont('Helvetica-Bold', 9)
					for h, x in zip(headers, x_positions):
						c.drawString(x, height - 1 * inch, h)
					c.setFont('Helvetica', 8)
					y = height - 1.2 * inch

				values = [
					str(row.get('Proposal Date', '')),
					str(row.get('Proposal ID', '')),
					str(row.get('Proposal Title', ''))[:60],
					str(row.get('Org Unit', '')),
					f"${row.get('USD Value', ''):,.2f}" if row.get('USD Value') is not None else '',
					str(row.get('Recipient', '')),
					str(row.get('Recipient Type', '')),
					str(row.get('Message Type', '')),
					str(row.get('Token Amount', '')),
					str(row.get('token_symbol', ''))
				]

				for v, x in zip(values, x_positions):
					try:
						c.drawString(x, y, v)
					except Exception:
						# Skip if unable to draw long text
						pass
				y -= 0.18 * inch
				rows_written += 1

		c.showPage()
		c.save()
		buffer.seek(0)
		return buffer.getvalue()