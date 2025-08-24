import pandas as pd
from typing import Dict, Any, Optional
import streamlit as st

class ReportGenerator:
    """Generate accounting reports from processed DAO proposal data"""
    
    def __init__(self):
        pass
    
    def generate_summary_stats(self, processed_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate summary statistics from processed data
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            Dictionary with summary statistics
        """
        if processed_data.empty:
            return {
                'total_payments': 0,
                'total_amount_osmo': 0.0,
                'core_team_payments': 0,
                'subunits_count': 0
            }
        
        total_payments = len(processed_data)
        total_amount_osmo = processed_data['Adjusted Amount'].sum()
        total_usd_value = processed_data['USD Value'].sum() if 'USD Value' in processed_data.columns else 0
        core_team_payments = len(processed_data[processed_data['Payment Type'] == 'Core Team'])
        subunits_count = processed_data['Sub-unit'].nunique()
        
        return {
            'total_payments': total_payments,
            'total_amount_osmo': total_amount_osmo,
            'total_usd_value': total_usd_value,
            'core_team_payments': core_team_payments,
            'subunits_count': subunits_count
        }
    
    def generate_subunit_summary(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate summary report grouped by sub-unit
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            DataFrame with sub-unit summary
        """
        if processed_data is None or processed_data.empty:
            return pd.DataFrame()

        summary = processed_data.groupby('Sub-unit').agg({
            'USD Value': ['sum', 'count', 'mean'],
            'Payment Type': lambda x: (x == 'Core Team').sum()
        }).round(2)

        # Flatten column names
        summary.columns = ['Total USD', 'Number of Payments', 'Average USD', 'Core Team Payments']
        summary = summary.reset_index()

        # Sort by total USD descending
        summary = summary.sort_values('Total USD', ascending=False)

        return summary
    
    def generate_core_team_breakdown(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate breakdown of core team vs non-core team payments
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            DataFrame with core team breakdown
        """
        if processed_data is None or processed_data.empty:
            return pd.DataFrame()

        breakdown = processed_data.groupby('Payment Type').agg({
            'USD Value': ['sum', 'count', 'mean']
        }).round(2)

        # Flatten column names
        breakdown.columns = ['Total USD', 'Number of Payments', 'Average USD']
        breakdown = breakdown.reset_index()
        breakdown = breakdown.rename(columns={'Payment Type': 'Type'})

        return breakdown
    
    def generate_detailed_report(self, processed_data: pd.DataFrame, include_zero_usd: bool = False) -> pd.DataFrame:
        """
        Generate detailed transaction report
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            DataFrame with detailed transactions
        """
        if processed_data.empty:
            return pd.DataFrame()
        
        # Select and reorder columns for the detailed report
        detailed_columns = [
            'Proposal ID',
            'Proposal Text',
            'Proposal Date',
            'Sub-unit',
            'Recipient',
            'Recipient Type',
            'Adjusted Amount',
            'Display Symbol',
            'USD Value',
            'Amount (uosmo)',
            'Payment Type',
            'Transaction Category',
            'Transaction Tag',
            'Amount Category',
            'Message Type',
            'Contract Method',
            'Contract Address',
            'Contract Title',
            'Contract Owner',
            'Denom'
        ]
        
        # Only include columns that exist in the data, but always include 'Proposal Date' if present
        available_columns = [col for col in detailed_columns if col in processed_data.columns]
        if 'Proposal Date' not in available_columns and 'Proposal Date' in processed_data.columns:
            available_columns.insert(2, 'Proposal Date')
        detailed_report = processed_data[available_columns].copy()
        
        # Sort by sub-unit and then by amount descending
        if len(detailed_report) > 0 and 'Sub-unit' in detailed_report.columns and 'Adjusted Amount' in detailed_report.columns:
            detailed_report = detailed_report.sort_values(by=['Sub-unit', 'Adjusted Amount'], ascending=[True, False])
        
        # Round amounts for better display
        if 'Adjusted Amount' in detailed_report.columns:
            detailed_report = detailed_report.copy()
            detailed_report['Adjusted Amount'] = detailed_report['Adjusted Amount'].round(6)

        # Filter out rows with missing proposal date or zero USD value
        if 'Proposal Date' in detailed_report.columns:
            detailed_report = detailed_report[detailed_report['Proposal Date'].notnull() & (detailed_report['Proposal Date'] != '')]

        if 'USD Value' in detailed_report.columns:
            # Coerce to numeric
            detailed_report['USD Value'] = pd.to_numeric(detailed_report['USD Value'], errors='coerce').fillna(0)
            # If the user does not want zero-USD rows, filter them out. Otherwise keep them.
            if not include_zero_usd:
                detailed_report = detailed_report[detailed_report['USD Value'] > 0]

        # Filter out specific message types we don't want to show
        if 'Message Type' in detailed_report.columns:
            detailed_report = detailed_report[detailed_report['Message Type'] != 'wasm_execute_funds']

        # Reset index after filtering
        detailed_report = detailed_report.reset_index(drop=True)

        # Build presentation columns requested by the UI/report
        # Ensure Proposal Title exists: prefer short title fields returned by API
        if 'Proposal Title' in detailed_report.columns:
            detailed_report['Proposal Title'] = detailed_report['Proposal Title'].fillna('').astype(str)
        elif 'proposal_title' in detailed_report.columns:
            detailed_report['Proposal Title'] = detailed_report['proposal_title'].fillna('').astype(str)
        elif 'title' in detailed_report.columns:
            detailed_report['Proposal Title'] = detailed_report['title'].fillna('').astype(str)
        elif 'Proposal Text' in detailed_report.columns:
            # Fallback to full proposal text when no short title exists
            detailed_report['Proposal Title'] = detailed_report['Proposal Text'].fillna('').astype(str)
        else:
            detailed_report['Proposal Title'] = ''

        # Org Unit (keep original 'Sub-unit' but surface as 'Org Unit')
        if 'Sub-unit' in detailed_report.columns:
            detailed_report['Org Unit'] = detailed_report['Sub-unit']
        else:
            detailed_report['Org Unit'] = ''

        # message type lowercase mapping
        if 'Message Type' in detailed_report.columns:
            detailed_report['message type'] = detailed_report['Message Type']
        else:
            detailed_report['message type'] = ''

        # Token amount: prefer Adjusted Amount, fall back to common amount columns
        amount_cols = ['Adjusted Amount', 'Amount (OSMO)', 'Amount (Raw)', 'Amount (uosmo)']
        for a in amount_cols:
            if a in detailed_report.columns:
                detailed_report['Token amount'] = detailed_report[a]
                break
        if 'Token amount' not in detailed_report.columns:
            detailed_report['Token amount'] = ''

        # token symbol (display symbol)
        if 'Display Symbol' in detailed_report.columns:
            detailed_report['token symbol'] = detailed_report['Display Symbol']
        elif 'Denom' in detailed_report.columns:
            detailed_report['token symbol'] = detailed_report['Denom']
        else:
            detailed_report['token symbol'] = ''

        # Ensure required columns exist so selection below won't KeyError
        required = ['Proposal Date', 'Proposal ID', 'Proposal Title', 'Org Unit', 'USD Value', 'Recipient', 'Recipient Type', 'message type', 'Token amount', 'token symbol']
        for r in required:
            if r not in detailed_report.columns:
                detailed_report[r] = ''

        # Coerce numeric types for USD Value and Token amount where possible
        if 'USD Value' in detailed_report.columns:
            detailed_report['USD Value'] = pd.to_numeric(detailed_report['USD Value'], errors='coerce').fillna(0)
        try:
            detailed_report['Token amount'] = pd.to_numeric(detailed_report['Token amount'], errors='coerce')
        except Exception:
            pass

        # Final column ordering and selection as requested
        final_columns = [
            'Proposal Date',
            'Proposal ID',
            'Proposal Title',
            'Org Unit',
            'USD Value',
            'Recipient',
            'Recipient Type',
            'message type',
            'Token amount',
            'token symbol'
        ]

        final_df = detailed_report[final_columns].copy()
        return final_df
    
    def generate_category_breakdown(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate breakdown by transaction categories
        """
        if processed_data is None or processed_data.empty or 'Transaction Category' not in processed_data.columns:
            return pd.DataFrame()

        category_breakdown = processed_data.groupby('Transaction Category').agg({
            'USD Value': ['sum', 'count', 'mean']
        }).round(2)

        # Flatten column names
        category_breakdown.columns = ['Total USD', 'Number of Payments', 'Average USD']
        category_breakdown = category_breakdown.reset_index()
        category_breakdown = category_breakdown.sort_values('Total USD', ascending=False)

        return category_breakdown
    
    def generate_amount_range_analysis(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate analysis by amount ranges
        """
        if processed_data is None or processed_data.empty or 'Amount Category' not in processed_data.columns:
            return pd.DataFrame()

        amount_analysis = processed_data.groupby('Amount Category').agg({
            'USD Value': ['sum', 'count'],
            'Sub-unit': 'nunique'
        }).round(2)

        # Flatten column names
        amount_analysis.columns = ['Total USD', 'Number of Transactions', 'Sub-units Involved']
        amount_analysis = amount_analysis.reset_index()

        # Custom sort order for amount categories
        category_order = [
            'Very Large (100K+ OSMO)',
            'Large (50K-100K OSMO)', 
            'Medium (10K-50K OSMO)',
            'Small (1K-10K OSMO)',
            'Minor (100-1K OSMO)',
            'Micro (<100 OSMO)'
        ]

        # Reorder based on category hierarchy
        sort_mapping = {cat: i for i, cat in enumerate(category_order)}
        amount_analysis['sort_order'] = amount_analysis['Amount Category'].map(sort_mapping)
        amount_analysis = amount_analysis.sort_values('sort_order').drop('sort_order', axis=1)

        return amount_analysis
    
    def generate_transaction_insights(self, processed_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate key insights from transaction data
        """
        if processed_data.empty:
            return {}
        
        insights = {}
        
        # Payment distribution insights (keep as Adjusted Amount percent if USD not available)
        if 'Payment Type' in processed_data.columns:
            core_team_amount = processed_data[processed_data['Payment Type'] == 'Core Team']['Adjusted Amount'].sum()
            total_amount = processed_data['Adjusted Amount'].sum()
            insights['core_team_percentage'] = (core_team_amount / total_amount * 100) if total_amount > 0 else 0
        
        # Largest transaction (prefer USD value when available)
        try:
            if 'USD Value' in processed_data.columns and processed_data['USD Value'].notnull().any():
                largest_idx = processed_data['USD Value'].idxmax()
                largest_tx = processed_data.loc[largest_idx]
                usd_val = float(largest_tx.get('USD Value', 0) or 0)
            else:
                largest_idx = processed_data['Adjusted Amount'].idxmax()
                largest_tx = processed_data.loc[largest_idx]
                usd_val = float(largest_tx.get('Adjusted Amount', 0) or 0)

            insights['largest_transaction'] = {
                'usd_value': usd_val,
                'amount': largest_tx.get('Adjusted Amount', 0),
                'recipient': largest_tx.get('Recipient', 'Unknown'),
                'sub_unit': largest_tx.get('Sub-unit', 'Unknown'),
                'category': largest_tx.get('Transaction Category', 'Unknown'),
                'symbol': largest_tx.get('Display Symbol', 'tokens')
            }
        except Exception:
            # If something goes wrong, skip largest transaction insight
            pass
        
        # Most frequent recipient (report total in USD when available)
        if 'Recipient' in processed_data.columns:
            recipient_counts = processed_data['Recipient'].value_counts()
            if len(recipient_counts) > 0:
                most_frequent_recipient = recipient_counts.index[0]
                try:
                    if 'USD Value' in processed_data.columns:
                        total_usd = float(processed_data[processed_data['Recipient'] == most_frequent_recipient]['USD Value'].sum() or 0)
                    else:
                        total_usd = float(processed_data[processed_data['Recipient'] == most_frequent_recipient]['Adjusted Amount'].sum() or 0)
                except Exception:
                    total_usd = 0

                insights['most_frequent_recipient'] = {
                    'address': most_frequent_recipient,
                    'count': recipient_counts.iloc[0],
                    'total_usd': total_usd
                }
        
        # Average transaction size by category
        if 'Transaction Category' in processed_data.columns:
            avg_by_category = processed_data.groupby('Transaction Category')['Adjusted Amount'].mean().to_dict()
            insights['avg_by_category'] = avg_by_category
        
        return insights
    
    def generate_recipient_summary(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate summary report grouped by recipient
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            DataFrame with recipient summary
        """
        if processed_data is None or processed_data.empty:
            return pd.DataFrame()

        recipient_summary = processed_data.groupby(['Recipient', 'Payment Type']).agg({
            'USD Value': ['sum', 'count'],
            'Sub-unit': 'nunique'
        }).round(2)

        # Flatten column names
        recipient_summary.columns = ['Total USD', 'Number of Payments', 'Number of Sub-units']
        recipient_summary = recipient_summary.reset_index()

        # Sort by total USD descending
        recipient_summary = recipient_summary.sort_values('Total USD', ascending=False)

        return recipient_summary
    
    def generate_monthly_summary(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate monthly summary report (if date information is available)
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            DataFrame with monthly summary
        """
        # This would require date information from proposals
        # For now, return empty DataFrame as date extraction is complex
        return pd.DataFrame()
    
    def export_to_csv(self, processed_data: pd.DataFrame, filename: Optional[str] = None) -> str:
        """
        Export processed data to CSV format
        
        Args:
            processed_data: DataFrame with processed payment information
            filename: Optional filename for the export
            
        Returns:
            CSV string data
        """
        if filename is None:
            from datetime import datetime
            filename = f"dao_accounting_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return processed_data.to_csv(index=False)
    
    def export_to_json(self, processed_data: pd.DataFrame) -> str:
        """
        Export processed data to JSON format
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            JSON string data
        """
        return processed_data.to_json(orient='records', indent=2) or '[]'

    def export_to_pdf(self, processed_data: pd.DataFrame, detailed_df: pd.DataFrame, title: str = 'DAO Accounting Report', include_zero_usd: bool = False) -> Optional[bytes]:
        """
        Export a full report PDF containing all page sections.

        Args:
            processed_data: full processed DataFrame
            detailed_df: filtered or full detailed DataFrame (as built by generate_detailed_report)
            title: report title
            include_zero_usd: whether zero-USD rows were included (for header note)

        Returns:
            PDF bytes or None on failure
        """
        try:
            # Import reportlab lazily to avoid hard dependency at module import
            from reportlab.lib.pagesizes import landscape, A4
            from reportlab.lib import colors
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
            from reportlab.lib.styles import getSampleStyleSheet
            import io

            styles = getSampleStyleSheet()
            buf = io.BytesIO()
            doc = SimpleDocTemplate(buf, pagesize=landscape(A4), leftMargin=18, rightMargin=18, topMargin=18, bottomMargin=18)
            story = []

            # Title
            story.append(Paragraph(title, styles['Title']))
            story.append(Spacer(1, 12))

            # Summary stats
            stats = self.generate_summary_stats(processed_data)
            total_payments = stats.get('total_payments', 0)
            total_usd = stats.get('total_usd_value', 0)
            core_team_cnt = stats.get('core_team_payments', 0)
            subunits_count = stats.get('subunits_count', 0)

            story.append(Paragraph(f"Total Transactions: {total_payments:,}", styles['Normal']))
            if total_usd and total_usd > 0:
                story.append(Paragraph(f"Total USD Value: ${total_usd:,.2f}", styles['Normal']))
            else:
                story.append(Paragraph(f"Total Amount: {stats.get('total_amount_osmo', 0):,.2f} (mixed tokens)", styles['Normal']))
            story.append(Paragraph(f"Core Team Transactions: {core_team_cnt:,}", styles['Normal']))
            story.append(Paragraph(f"Sub-DAOs: {subunits_count:,}", styles['Normal']))
            story.append(Spacer(1, 12))

            # Key insights
            insights = self.generate_transaction_insights(processed_data)
            story.append(Paragraph("Key Insights", styles['Heading2']))
            story.append(Spacer(1, 6))
            if insights:
                # Largest transaction
                largest = insights.get('largest_transaction')
                if largest:
                    usd_val = largest.get('usd_value')
                    if usd_val is not None and usd_val > 0:
                        story.append(Paragraph(f"Largest Transaction: ${usd_val:,.2f} to {str(largest.get('recipient',''))[:40]}... ({largest.get('category','')})", styles['Normal']))
                    else:
                        story.append(Paragraph(f"Largest Transaction: {largest.get('amount',0):,.2f} {largest.get('symbol','tokens')} to {str(largest.get('recipient',''))[:40]}... ({largest.get('category','')})", styles['Normal']))

                # Most frequent recipient
                frequent = insights.get('most_frequent_recipient')
                if frequent:
                    total_usd_rec = frequent.get('total_usd')
                    if total_usd_rec is not None:
                        story.append(Paragraph(f"Most Frequent Recipient: {frequent.get('count',0)} transactions totaling ${total_usd_rec:,.2f}", styles['Normal']))
                    else:
                        story.append(Paragraph(f"Most Frequent Recipient: {frequent.get('count',0)} transactions", styles['Normal']))

            story.append(Spacer(1, 12))

            # Helper to add a small dataframe table
            def add_table_from_df(df: pd.DataFrame, max_cols=8, title_text=None):
                if df is None or df.empty:
                    return
                if title_text:
                    story.append(Paragraph(title_text, styles['Heading3']))
                # Limit number of columns shown to avoid layout overflow
                cols = list(df.columns[:max_cols])
                data = [cols]
                # format rows
                for _, r in df.iterrows():
                    row = []
                    for c in cols:
                        val = r.get(c, '')
                        if pd.isna(val):
                            val = ''
                        # Format floats
                        if isinstance(val, float):
                            val = f"{val:,.2f}"
                        row.append(str(val))
                    data.append(row)

                tbl = Table(data, repeatRows=1)
                tbl_style = TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f2f2f2')),
                    ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('FONTSIZE', (0, 0), (-1, -1), 8),
                ])
                tbl.setStyle(tbl_style)
                story.append(tbl)
                story.append(Spacer(1, 12))

            # Payments by sub-unit
            try:
                subunit_summary = self.generate_subunit_summary(processed_data)
                add_table_from_df(subunit_summary, max_cols=4, title_text='Payments by Sub-unit (USD)')
            except Exception:
                pass

            # Category breakdown
            try:
                cat = self.generate_category_breakdown(processed_data)
                add_table_from_df(cat, max_cols=4, title_text='Transaction Categories (USD)')
            except Exception:
                pass

            # Amount range analysis
            try:
                amt = self.generate_amount_range_analysis(processed_data)
                add_table_from_df(amt, max_cols=4, title_text='Amount Range Analysis (USD)')
            except Exception:
                pass

            # Core team breakdown
            try:
                core = self.generate_core_team_breakdown(processed_data)
                add_table_from_df(core, max_cols=4, title_text='Core Team vs Non-Core Team (USD)')
            except Exception:
                pass

            # Transaction tags (summary of top tags)
            try:
                if 'Transaction Tag' in processed_data.columns:
                    tags_data = []
                    for _, row in processed_data.iterrows():
                        tag_value = row['Transaction Tag']
                        if isinstance(tag_value, str):
                            tags = tag_value.split(' | ')
                        else:
                            tags = [str(tag_value)]
                        for tag in tags:
                            tags_data.append({'Tag': tag, 'USD Value': row.get('USD Value', 0)})
                    tags_df = pd.DataFrame(tags_data)
                    if not tags_df.empty:
                        tag_summary = tags_df.groupby('Tag').agg({'USD Value': ['sum', 'count']}).round(2)
                        tag_summary.columns = ['Total USD', 'Count']
                        tag_summary = tag_summary.reset_index().sort_values('Total USD', ascending=False)
                        add_table_from_df(tag_summary, max_cols=4, title_text='Top Transaction Tags by USD')
            except Exception:
                pass

            # Detailed transactions table (paginate)
            story.append(PageBreak())
            story.append(Paragraph('Detailed Transactions', styles['Heading2']))
            story.append(Spacer(1, 6))

            # Prepare detailed rows and paginate (25 rows per page)
            rows_per_page = 25
            if detailed_df is None or detailed_df.empty:
                story.append(Paragraph('No detailed transactions available.', styles['Normal']))
            else:
                # Ensure columns exist and are strings for presentation
                display_df = detailed_df.copy()
                # Truncate long text fields
                for col in display_df.columns:
                    display_df[col] = display_df[col].apply(lambda x: (str(x)[:120] + '...') if isinstance(x, str) and len(x) > 120 else x)

                headers = list(display_df.columns)
                total_rows = len(display_df)
                for start in range(0, total_rows, rows_per_page):
                    chunk = display_df.iloc[start:start+rows_per_page]
                    data = [headers]
                    for _, r in chunk.iterrows():
                        row = []
                        for h in headers:
                            v = r.get(h, '')
                            if pd.isna(v):
                                v = ''
                            if isinstance(v, float):
                                v = f"{v:,.2f}"
                            row.append(str(v))
                        data.append(row)

                    tbl = Table(data, repeatRows=1)
                    tbl.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f2f2f2')),
                        ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('FONTSIZE', (0, 0), (-1, -1), 7),
                    ]))
                    story.append(tbl)
                    story.append(Spacer(1, 12))
                    if start + rows_per_page < total_rows:
                        story.append(PageBreak())

            # Build PDF
            doc.build(story)
            pdf_bytes = buf.getvalue()
            buf.close()
            return pdf_bytes

        except Exception as e:
            try:
                st.warning(f"PDF export failed: {e}")
            except Exception:
                pass
            return None

    def export_to_pdf(self, processed_data: pd.DataFrame, detailed_df: Optional[pd.DataFrame] = None, filename: Optional[str] = None, title: str = "DAO Accounting Report", include_zero_usd: bool = False, subdaos: Optional[list] = None, main_dao: Optional[str] = None, core_team: Optional[list] = None, proposals_count: Optional[int] = None) -> bytes:
        """
        Export the detailed report to a PDF and return bytes.

        Requires: reportlab (pip install reportlab)
        """
        if processed_data is None or processed_data.empty:
            return b''

        # Use provided detailed DataFrame if given (avoids filtering applied in generate_detailed_report)
        if detailed_df is not None:
            detailed = detailed_df.copy()
        else:
            detailed = self.generate_detailed_report(processed_data)

        if detailed is None or detailed.empty:
            return b''

        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
            from reportlab.lib import colors
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.enums import TA_LEFT
            from io import BytesIO
        except Exception as e:
            raise ImportError("reportlab is required to export PDF. Install with: pip install reportlab") from e

        buf = BytesIO()
        page_size = landscape(A4)
        left_margin = 18
        right_margin = 18
        top_margin = 18
        bottom_margin = 18
        doc = SimpleDocTemplate(buf, pagesize=page_size, rightMargin=right_margin, leftMargin=left_margin, topMargin=top_margin, bottomMargin=bottom_margin)
        styles = getSampleStyleSheet()
        body_style = ParagraphStyle('body', parent=styles['Normal'], fontSize=8, leading=10, alignment=TA_LEFT)
        title_style = styles.get('Title', styles['Normal'])
        heading_style = styles.get('Heading2', styles['Normal'])
        elems = []

        # --- Header Page with parameters ---
        elems.append(Paragraph(title, title_style))
        elems.append(Spacer(1, 12))

        # Parameters summary (subdaos, main dao, core team, number of proposals)
        elems.append(Paragraph('Report Parameters', heading_style))
        elems.append(Spacer(1, 6))
        # Sub-DAOs
        subdaos_display = ', '.join(subdaos) if subdaos else ', '.join(sorted(list(set(processed_data.get('Sub-unit', []).tolist()))))
        elems.append(Paragraph(f"Sub-DAOs: {subdaos_display}", body_style))
        # Main DAO
        if main_dao:
            elems.append(Paragraph(f"Main DAO: {main_dao}", body_style))
        # Core team
        if core_team:
            core_preview = ', '.join(core_team[:10]) + ('' if len(core_team) <= 10 else f" (+{len(core_team)-10} more)")
            elems.append(Paragraph(f"Core Team Members (sample): {core_preview}", body_style))
        # Proposals count
        if proposals_count is None:
            # try to infer from processed_data if available
            try:
                proposals_count = int(processed_data['Proposal ID'].nunique())
            except Exception:
                proposals_count = None
        if proposals_count is not None:
            elems.append(Paragraph(f"Proposals Reviewed: {proposals_count}", body_style))

        elems.append(Spacer(1, 12))

        # Key Insights (all)
        elems.append(Paragraph('Key Insights', heading_style))
        elems.append(Spacer(1, 6))
        insights = self.generate_transaction_insights(processed_data)
        if insights:
            # Print each insight key/value in a readable way
            for k, v in insights.items():
                try:
                    elems.append(Paragraph(f"{k.replace('_',' ').title()}: {str(v)}", body_style))
                except Exception:
                    elems.append(Paragraph(f"{k}: (unavailable)", body_style))
        else:
            elems.append(Paragraph('No key insights available.', body_style))

        elems.append(PageBreak())

        # --- Second section: Breakdowns and charts ---
        elems.append(Paragraph('Breakdowns', heading_style))
        elems.append(Spacer(1, 8))

        # Helper: convert plotly figure to image bytes (PNG) if available
        def fig_to_png_bytes(fig):
            try:
                # Try using plotly's to_image (requires kaleido)
                img_bytes = fig.to_image(format='png')
                return img_bytes
            except Exception:
                try:
                    # fallback to write_image
                    import io as _io
                    buf_img = _io.BytesIO()
                    fig.write_image(buf_img, format='png')
                    return buf_img.getvalue()
                except Exception:
                    return None

        # Helper to add an image to elems if bytes available
        def add_image_bytes(img_bytes, max_width_ratio=0.9):
            if not img_bytes:
                return
            try:
                from reportlab.platypus import Image as RLImage
                import io as _io
                img_buf = _io.BytesIO(img_bytes)
                # estimate width
                page_w, _ = page_size
                usable_w = page_w - left_margin - right_margin
                img = RLImage(img_buf, width=usable_w * max_width_ratio)
                elems.append(img)
                elems.append(Spacer(1, 8))
            except Exception:
                return

        # Try to create plotly charts; if plotly/kaleido not available, skip charts gracefully
        try:
            import plotly.express as px
            has_plotly = True
        except Exception:
            has_plotly = False

        def add_table_from_df_local(df: pd.DataFrame, max_cols=8, title_text=None):
            # local version to reuse styles/spacing in this function
            if df is None or df.empty:
                return
            if title_text:
                elems.append(Paragraph(title_text, styles.get('Heading3', styles['Normal'])))
            cols = list(df.columns[:max_cols])
            data = [cols]
            for _, r in df.iterrows():
                row = []
                for c in cols:
                    val = r.get(c, '')
                    if pd.isna(val):
                        val = ''
                    if isinstance(val, float):
                        val = f"{val:,.2f}"
                    row.append(str(val))
                data.append(row)
            tbl = Table(data, repeatRows=1)
            tbl_style = TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f2f2f2')),
                ('GRID', (0, 0), (-1, -1), 0.25, colors.grey),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
            ])
            tbl.setStyle(tbl_style)
            elems.append(tbl)
            elems.append(Spacer(1, 12))

        # Payments by sub-unit: table + chart
        subunit_summary = self.generate_subunit_summary(processed_data)
        add_table_from_df_local(subunit_summary, max_cols=4, title_text='Payments by Sub-unit (USD)')
        if has_plotly and subunit_summary is not None and not subunit_summary.empty:
            try:
                fig = px.bar(subunit_summary, x='Sub-unit', y='Total USD', title='Payments by Sub-unit (USD)', text='Number of Payments')
                img = fig_to_png_bytes(fig)
                add_image_bytes(img)
            except Exception:
                pass

        # Then each requested breakdown with chart then details
        # Transaction Categories
        cat = self.generate_category_breakdown(processed_data)
        if has_plotly and cat is not None and not cat.empty:
            try:
                fig = px.pie(cat, names='Transaction Category', values='Total USD', title='Transaction Categories by USD')
                img = fig_to_png_bytes(fig)
                add_image_bytes(img)
            except Exception:
                pass
        add_table_from_df_local(cat, max_cols=4, title_text='💰 Payment Categories (USD)')

        # Amount Ranges
        amt = self.generate_amount_range_analysis(processed_data)
        if has_plotly and amt is not None and not amt.empty:
            try:
                fig = px.bar(amt, x='Amount Category', y='Total USD', title='Amount Ranges (USD)')
                img = fig_to_png_bytes(fig)
                add_image_bytes(img)
            except Exception:
                pass
        add_table_from_df_local(amt, max_cols=4, title_text='📊 Amount Ranges')

        # Core Team Analysis
        core = self.generate_core_team_breakdown(processed_data)
        if has_plotly and core is not None and not core.empty:
            try:
                fig = px.pie(core, names='Type', values='Total USD', title='Core Team vs Non-Core (USD)')
                img = fig_to_png_bytes(fig)
                add_image_bytes(img)
            except Exception:
                pass
        add_table_from_df_local(core, max_cols=4, title_text='👥 Core Team Analysis')

        # Transaction Tags
        try:
            if 'Transaction Tag' in processed_data.columns:
                tags_data = []
                for _, row in processed_data.iterrows():
                    tag_value = row['Transaction Tag']
                    if isinstance(tag_value, str):
                        tags = tag_value.split(' | ')
                    else:
                        tags = [str(tag_value)]
                    for tag in tags:
                        tags_data.append({'Tag': tag, 'USD Value': row.get('USD Value', 0)})
                tags_df = pd.DataFrame(tags_data)
                if not tags_df.empty:
                    tag_summary = tags_df.groupby('Tag').agg({'USD Value': ['sum', 'count']}).round(2)
                    tag_summary.columns = ['Total USD', 'Count']
                    tag_summary = tag_summary.reset_index().sort_values('Total USD', ascending=False)
                    if has_plotly and not tag_summary.empty:
                        try:
                            fig = px.bar(tag_summary.head(20), x='Tag', y='Total USD', title='Top Transaction Tags by USD')
                            img = fig_to_png_bytes(fig)
                            add_image_bytes(img, max_width_ratio=0.95)
                        except Exception:
                            pass
                    add_table_from_df_local(tag_summary, max_cols=4, title_text='🏷️ Transaction Tags')
        except Exception:
            pass

        elems.append(PageBreak())

        # --- Detailed transactions (kept as the final section) ---
        elems.append(Paragraph('Detailed Transactions', heading_style))
        elems.append(Spacer(1, 6))
        final_columns = [
            'Proposal Date',
            'Proposal ID',
            'Proposal Title',
            'Org Unit',
            'USD Value',
            'Recipient',
            'Recipient Type',
            'message type',
            'Token amount',
            'token symbol'
        ]

        detailed_pdf = detailed.copy()

        # Ensure Proposal Title exists and prefer existing short title fields
        if 'Proposal Title' not in detailed_pdf.columns and 'proposal_title' in detailed_pdf.columns:
            detailed_pdf['Proposal Title'] = detailed_pdf['proposal_title']
        if 'Proposal Title' not in detailed_pdf.columns and 'title' in detailed_pdf.columns:
            detailed_pdf['Proposal Title'] = detailed_pdf['title']
        if 'Proposal Title' not in detailed_pdf.columns and 'Proposal Text' in detailed_pdf.columns:
            detailed_pdf['Proposal Title'] = detailed_pdf['Proposal Text']

        # Ensure Org Unit exists (map from Sub-unit)
        if 'Org Unit' not in detailed_pdf.columns and 'Sub-unit' in detailed_pdf.columns:
            detailed_pdf['Org Unit'] = detailed_pdf['Sub-unit']

        # Ensure message type lowercase column exists (map from Message Type)
        if 'message type' not in detailed_pdf.columns and 'Message Type' in detailed_pdf.columns:
            detailed_pdf['message type'] = detailed_pdf['Message Type']

        # Token amount: prefer Token amount or Adjusted Amount
        if 'Token amount' not in detailed_pdf.columns:
            for a in ['Token amount', 'Adjusted Amount', 'Amount (OSMO)', 'Amount (Raw)', 'Amount (uosmo)']:
                if a in detailed_pdf.columns:
                    detailed_pdf['Token amount'] = detailed_pdf[a]
                    break
        # token symbol: prefer Display Symbol then Denom
        if 'token symbol' not in detailed_pdf.columns:
            if 'Display Symbol' in detailed_pdf.columns:
                detailed_pdf['token symbol'] = detailed_pdf['Display Symbol']
            elif 'Denom' in detailed_pdf.columns:
                detailed_pdf['token symbol'] = detailed_pdf['Denom']
            else:
                detailed_pdf['token symbol'] = ''

        # Ensure USD Value present and numeric
        if 'USD Value' not in detailed_pdf.columns:
            detailed_pdf['USD Value'] = 0
        detailed_pdf['USD Value'] = pd.to_numeric(detailed_pdf['USD Value'], errors='coerce').fillna(0)

        # Ensure all final columns exist (create empty if missing)
        for c in final_columns:
            if c not in detailed_pdf.columns:
                detailed_pdf[c] = ''

        # Prepare header from final_columns
        header = [str(col) for col in final_columns]

        # Truncate very long content for the final PDF columns to avoid overly tall table cells.
        # We respect a larger limit for short proposal titles but still guard against pathological cases.
        detailed_limited = detailed_pdf.copy()
        # Normalize and truncate the final columns (but allow Proposal Title more room)
        max_chars_default = 300
        max_chars_map = {
            'Proposal Title': 800,  # allow wider short titles up to a reasonable limit
            'Recipient': 120,
            'Org Unit': 200,
            'message type': 80,
            'token symbol': 60,
            'Recipient Type': 60,
            'Proposal Date': 40,
            'Proposal ID': 20,
            'USD Value': 40,
            'Token amount': 60
        }

        for col in final_columns:
            if col not in detailed_limited.columns:
                detailed_limited[col] = ''
            # Normalize to string for safe handling
            detailed_limited[col] = detailed_limited[col].fillna('').astype(str).apply(lambda s: s.replace('\n', ' '))
            max_c = max_chars_map.get(col, max_chars_default)
            detailed_limited[col] = detailed_limited[col].apply(lambda s, mc=max_c: (s if len(s) <= mc else s[:mc-3] + '...'))

        # Estimate column widths based on page width and allocate sensible proportions to wide fields
        page_width, page_height = page_size
        usable_width = page_width - left_margin - right_margin

        # Assign column width proportions (total should be ~1.0)
        # Favor Proposal Title and Recipient with larger widths, USD and IDs smaller
        col_percents = {
            'Proposal Date': 0.08,
            'Proposal ID': 0.06,
            'Proposal Title': 0.26,
            'Org Unit': 0.14,
            'USD Value': 0.08,
            'Recipient': 0.18,
            'Recipient Type': 0.06,
            'message type': 0.06,
            'Token amount': 0.06,
            'token symbol': 0.04
        }
        # Fallback equal distribution if keys missing
        col_widths = []
        for c in header:
            pct = col_percents.get(c, 1.0 / len(header))
            col_widths.append(max(40, usable_width * pct))

        data = [header]

        # Convert values to Paragraphs for wrapping and format numeric token amounts
        for _, row in detailed_limited.iterrows():
            row_vals = []
            for col in final_columns:
                val = row.get(col, '')
                if pd.isna(val):
                    s = ''
                else:
                    # Format numeric values nicely
                    if col in ('USD Value', 'Token amount'):
                        try:
                            if str(val).strip() == '':
                                s = ''
                            else:
                                if col == 'Token amount':
                                    s = f"{float(val):,.6f}"
                                else:
                                    s = f"${float(val):,.2f}"
                        except Exception:
                            s = str(val)
                    else:
                        s = str(val)
                # Use Paragraph to allow text wrapping inside table cell
                p = Paragraph(s.replace('\n', '<br/>'), body_style)
                row_vals.append(p)
            data.append(row_vals)

        # Convert header cells to Paragraph for consistent styling
        header_par = [Paragraph(h, ParagraphStyle('h', parent=styles['Normal'], fontSize=7, leading=9, alignment=TA_LEFT)) for h in header]
        data[0] = header_par

        table = Table(data, colWidths=col_widths, repeatRows=1)
        style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4B5563')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('GRID', (0, 0), (-1, -1), 0.25, colors.black),
        ])
        table.setStyle(style)

        elems.append(table)
        doc.build(elems)

        pdf_bytes = buf.getvalue()
        buf.close()
        return pdf_bytes
