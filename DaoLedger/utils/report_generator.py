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
        if processed_data.empty:
            return pd.DataFrame()
        
        summary = processed_data.groupby('Sub-unit').agg({
            'Adjusted Amount': ['sum', 'count', 'mean'],
            'Payment Type': lambda x: (x == 'Core Team').sum()
        }).round(2)
        
        # Flatten column names
        summary.columns = ['Total Amount', 'Number of Payments', 'Average Payment', 'Core Team Payments']
        summary = summary.reset_index()
        
        # Sort by total amount descending
        summary = summary.sort_values('Total Amount', ascending=False)
        
        return summary
    
    def generate_core_team_breakdown(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate breakdown of core team vs non-core team payments
        
        Args:
            processed_data: DataFrame with processed payment information
            
        Returns:
            DataFrame with core team breakdown
        """
        if processed_data.empty:
            return pd.DataFrame()
        
        breakdown = processed_data.groupby('Payment Type').agg({
            'Adjusted Amount': ['sum', 'count', 'mean']
        }).round(2)
        
        # Flatten column names
        breakdown.columns = ['Total Amount', 'Number of Payments', 'Average Payment']
        breakdown = breakdown.reset_index()
        breakdown = breakdown.rename(columns={'Payment Type': 'Type'})
        
        return breakdown
    
    def generate_detailed_report(self, processed_data: pd.DataFrame) -> pd.DataFrame:
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
        
        return detailed_report
    
    def generate_category_breakdown(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate breakdown by transaction categories
        """
        if processed_data.empty or 'Transaction Category' not in processed_data.columns:
            return pd.DataFrame()
        
        category_breakdown = processed_data.groupby('Transaction Category').agg({
            'Adjusted Amount': ['sum', 'count', 'mean']
        }).round(2)
        
        # Flatten column names
        category_breakdown.columns = ['Total Amount', 'Number of Payments', 'Average Payment']
        category_breakdown = category_breakdown.reset_index()
        category_breakdown = category_breakdown.sort_values('Total Amount', ascending=False)
        
        return category_breakdown
    
    def generate_amount_range_analysis(self, processed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Generate analysis by amount ranges
        """
        if processed_data.empty or 'Amount Category' not in processed_data.columns:
            return pd.DataFrame()
        
        amount_analysis = processed_data.groupby('Amount Category').agg({
            'Adjusted Amount': ['sum', 'count'],
            'Sub-unit': 'nunique'
        }).round(2)
        
        # Flatten column names
        amount_analysis.columns = ['Total Amount', 'Number of Transactions', 'Sub-units Involved']
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
        
        # Payment distribution insights
        if 'Payment Type' in processed_data.columns:
            core_team_amount = processed_data[processed_data['Payment Type'] == 'Core Team']['Adjusted Amount'].sum()
            total_amount = processed_data['Adjusted Amount'].sum()
            insights['core_team_percentage'] = (core_team_amount / total_amount * 100) if total_amount > 0 else 0
        
        # Largest transaction
        if 'Adjusted Amount' in processed_data.columns:
            largest_tx = processed_data.loc[processed_data['Adjusted Amount'].idxmax()]
            insights['largest_transaction'] = {
                'amount': largest_tx['Adjusted Amount'],
                'recipient': largest_tx.get('Recipient', 'Unknown'),
                'sub_unit': largest_tx.get('Sub-unit', 'Unknown'),
                'category': largest_tx.get('Transaction Category', 'Unknown'),
                'symbol': largest_tx.get('Display Symbol', 'tokens')
            }
        
        # Most frequent recipient
        if 'Recipient' in processed_data.columns:
            recipient_counts = processed_data['Recipient'].value_counts()
            if len(recipient_counts) > 0:
                most_frequent_recipient = recipient_counts.index[0]
                insights['most_frequent_recipient'] = {
                    'address': most_frequent_recipient,
                    'count': recipient_counts.iloc[0],
                    'total_amount': processed_data[processed_data['Recipient'] == most_frequent_recipient]['Adjusted Amount'].sum()
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
        if processed_data.empty:
            return pd.DataFrame()
        
        recipient_summary = processed_data.groupby(['Recipient', 'Payment Type']).agg({
            'Adjusted Amount': ['sum', 'count'],
            'Sub-unit': 'nunique'
        }).round(2)
        
        # Flatten column names
        recipient_summary.columns = ['Total Amount', 'Number of Payments', 'Number of Sub-units']
        recipient_summary = recipient_summary.reset_index()
        
        # Sort by total amount descending
        recipient_summary = recipient_summary.sort_values('Total Amount', ascending=False)
        
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
