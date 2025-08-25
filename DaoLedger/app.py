import pandas as pd
import streamlit as st
import json
import requests
from datetime import datetime
import plotly
import streamlit as st
import pandas as pd
import json
import requests
from datetime import datetime
try:
    import plotly.express as px
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except Exception:
    px = None
    go = None
    HAS_PLOTLY = False
from utils.api_client import DAOAPIClient
from utils.data_processor import DataProcessor
from utils.report_generator import ReportGenerator

@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_token_data():
    """Load token data from Cosmostation chainlist"""
    try:
        url = "https://raw.githubusercontent.com/cosmostation/chainlist/refs/heads/main/chain/osmosis/assets_2.json"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        assets_data = response.json()
        token_info = {}
        
        # The JSON is a direct array of token objects
        for asset in assets_data:
            denom = asset.get('denom', '')
            symbol = asset.get('symbol', denom)
            decimals = asset.get('decimals', 0)
            
            if denom:
                token_info[denom] = {
                    'symbol': symbol,
                    'decimals': decimals
                }
        
        return token_info
        
    except Exception as e:
        st.warning(f"Could not load token data: {e}")
        return {}

def main():
    st.set_page_config(
        page_title="DAO Accounting Reports",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 DAO Organizational Accounting Reports")
    st.markdown("Generate accounting reports for DAO organizational sub-units from proposal data")
    
    # Initialize session state
    if 'proposal_data' not in st.session_state:
        st.session_state.proposal_data = {}
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'subdaos' not in st.session_state:
        st.session_state.subdaos = []
    if 'selected_subdaos' not in st.session_state:
        st.session_state.selected_subdaos = {}
    if 'main_dao_info' not in st.session_state:
        st.session_state.main_dao_info = {}
    if 'main_dao_address' not in st.session_state:
        st.session_state.main_dao_address = ""
    if 'include_main_dao' not in st.session_state:
        st.session_state.include_main_dao = False
    if 'initial_load' not in st.session_state:
        st.session_state.initial_load = True
    if 'token_data' not in st.session_state:
        st.session_state.token_data = {}
    
    # Load token data on startup
    if not st.session_state.token_data:
        with st.spinner("Loading token information..."):
            st.session_state.token_data = load_token_data()
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # API Configuration
        st.subheader("API Settings")
        api_base_url = st.text_input(
            "API Base URL",
            value="https://indexer.daodao.zone",
            help="Base URL for the DAO DAO indexer API"
        )
        
        network = st.selectbox(
            "Network",
            ["osmosis-1", "juno-1", "stargaze-1"],
            index=0,
            help="Blockchain network to query"
        )
        
        # Main DAO Configuration
        st.subheader("Main DAO Configuration")
        main_dao_address = st.text_input(
            "Main DAO Address",
            value="osmo1a40j922z0kwqhw2nn0nx66ycyk88vyzcs73fyjrd092cjgyvyjksrd8dp7",
            placeholder="osmo1...",
            help="Enter the main DAO contract address"
        )
        # persist the input value in session state so other UI areas can access it
        st.session_state.main_dao_address = main_dao_address

        # Option to include the main DAO itself when fetching proposals
        st.session_state.include_main_dao = st.checkbox(
            "Include Main DAO in proposal fetch",
            value=st.session_state.include_main_dao,
            help="When enabled, the main DAO's own proposals will be fetched in addition to selected sub-DAOs"
        )
        
        if st.button("🔄 Update DAO Info"):
            st.session_state.initial_load = True
        
        # Core Team Configuration
        st.subheader("Core Team Members")
        
        # Auto-fetch core team members
        if st.session_state.subdaos:
            auto_fetch_core_team = st.button("🔄 Auto-fetch Core Team Members", help="Automatically fetch core team members from the Core Team sub-DAO")
            
            if auto_fetch_core_team:
                # Look for Core Team sub-DAO
                core_team_dao = None
                for subdao in st.session_state.subdaos:
                    subdao_name = (subdao.get('name', '') or 
                                  subdao.get('dao_name', '') or 
                                  subdao.get('config', {}).get('name', '')).lower()
                    subdao_addr = subdao.get('addr', subdao.get('address', ''))
                    
                    # Check if this is the core team DAO
                    if 'core team' in subdao_name or subdao_addr == 'osmo18pl3nq7r5xht260jsm245j3c8xjhu2nd7ucasllfj4waqehrw3zsll9zgq':
                        core_team_dao = subdao_addr
                        break
                
                if core_team_dao:
                    with st.spinner("Fetching core team members..."):
                        try:
                            api_client = DAOAPIClient(api_base_url, network)
                            members = api_client.get_dao_members(core_team_dao)
                            
                            # Extract addresses from members
                            member_addresses = []
                            for member in members:
                                # Handle different member data formats
                                addr = (member.get('addr') or 
                                       member.get('address') or 
                                       member.get('member', {}).get('addr') or
                                       member.get('member', {}).get('address'))
                                if addr:
                                    member_addresses.append(addr)
                            
                            if member_addresses:
                                st.session_state.auto_core_team = '\n'.join(member_addresses)
                                st.success(f"✅ Found {len(member_addresses)} core team members")
                            else:
                                st.warning("No core team members found")
                                
                        except Exception as e:
                            st.error(f"❌ Error fetching core team members: {str(e)}")
                else:
                    st.warning("Core Team sub-DAO not found. Please check that it exists in the sub-DAOs list.")
        
        # Initialize auto core team in session state
        if 'auto_core_team' not in st.session_state:
            st.session_state.auto_core_team = ""
        
        st.markdown("**Core team member addresses:**")
        core_team_input = st.text_area(
            "Core Team Addresses (one per line)",
            value=st.session_state.auto_core_team,
            placeholder="osmo1...\nosmo2...\nosmo3...",
            help="Core team addresses (auto-populated from Core Team sub-DAO or enter manually)"
        )
        
        core_team_addresses = []
        if core_team_input:
            core_team_addresses = [addr.strip() for addr in core_team_input.split('\n') if addr.strip()]

        # Debug toggle for showing raw proposal payloads
        if 'debug' not in st.session_state:
            st.session_state.debug = False
        st.session_state.debug = st.checkbox("Enable debug mode (show raw proposals)", value=st.session_state.debug)
        # Option to include zero-USD or missing-date rows in reports and PDF exports
        if 'include_zero_usd' not in st.session_state:
            st.session_state.include_zero_usd = False
        st.session_state.include_zero_usd = st.checkbox(
            "Include zero-USD / missing-price rows in reports and exports",
            value=st.session_state.include_zero_usd,
            help="When enabled, rows with no USD price (or USD == 0) will be included in detailed tables and PDF exports."
        )
    
    # Auto-fetch DAO info and sub-DAOs on initial load or when main DAO changes
    if st.session_state.initial_load and main_dao_address:
        with st.spinner("Loading DAO information and sub-DAOs..."):
            try:
                api_client = DAOAPIClient(api_base_url, network)
                
                # Fetch main DAO info
                main_dao_info = api_client.get_dao_info(main_dao_address)
                st.session_state.main_dao_info = main_dao_info
                
                # Fetch sub-DAOs
                subdaos_data = api_client.get_subdaos(main_dao_address)
                st.session_state.subdaos = subdaos_data
                
                # Initialize selection state for new sub-DAOs
                for subdao in subdaos_data:
                    subdao_addr = subdao.get('addr', subdao.get('address', ''))
                    if subdao_addr not in st.session_state.selected_subdaos:
                        st.session_state.selected_subdaos[subdao_addr] = False
                        
                st.session_state.initial_load = False
                st.success(f"✅ Loaded {len(subdaos_data)} sub-DAOs from {main_dao_info.get('config', {}).get('name', 'Main DAO')}")
                
            except Exception as e:
                st.error(f"❌ Error loading DAO information: {str(e)}")
                st.session_state.subdaos = []
                st.session_state.main_dao_info = {}
    
    # Main content area
    if st.session_state.main_dao_info:
        main_dao_name = st.session_state.main_dao_info.get('config', {}).get('name', 'Main DAO')
        st.subheader(f"📋 {main_dao_name} - Sub-DAO Management")
    else:
        st.subheader("📋 Sub-DAO Management")
    
    # Sub-DAO Selection in main area
    subunits = {}
    
    if st.session_state.subdaos:
        st.markdown("**Select sub-DAOs to include in the accounting report:**")
        
        # Create columns for better layout
        cols = st.columns(3)
        col_idx = 0
        
        for subdao in st.session_state.subdaos:
            # Extract name and address from sub-DAO data
            subdao_name = (subdao.get('name') or 
                          subdao.get('dao_name') or 
                          subdao.get('config', {}).get('name') or
                          subdao.get('info', {}).get('name') or
                          f"DAO {subdao.get('addr', subdao.get('address', ''))[:8]}...")
                          
            subdao_addr = subdao.get('addr', subdao.get('address', ''))
            
            if subdao_addr:
                # Try to fetch DAO name if we don't have it
                if subdao_name.startswith('DAO ') and main_dao_address:
                    try:
                        api_client = DAOAPIClient(api_base_url, network)
                        dao_state = api_client.get_dao_info(subdao_addr)
                        
                        # Extract name from dumpState response
                        name = (dao_state.get('config', {}).get('name') or
                               dao_state.get('name') or
                               dao_state.get('dao_name') or
                               dao_state.get('info', {}).get('name'))
                        
                        if name:
                            subdao_name = name
                    except:
                        pass
                
                # Create checkbox for each sub-DAO in columns
                with cols[col_idx % 3]:
                    selected = st.checkbox(
                        f"{subdao_name}",
                        value=st.session_state.selected_subdaos.get(subdao_addr, False),
                        key=f"subdao_{subdao_addr}",
                        help=f"Address: {subdao_addr}"
                    )
                    
                    # Update selection state
                    st.session_state.selected_subdaos[subdao_addr] = selected
                    
                    # Add to subunits if selected
                    if selected:
                        subunits[subdao_name] = subdao_addr
                
                col_idx += 1
        
        # Summary of selected sub-DAOs
        # If the user requested including the Main DAO, add it to the set of subunits to fetch
        if st.session_state.get('include_main_dao') and st.session_state.get('main_dao_address'):
            # Use a fixed key so downstream code can reliably find the main DAO entry
            main_key = 'Main DAO'
            subunits[main_key] = st.session_state.main_dao_address

        if subunits:
            st.markdown("---")
            st.markdown(f"**Selected Sub-DAOs ({len(subunits)}):**")
            selected_names = list(subunits.keys())
            st.write(", ".join(selected_names))
        
        # Action buttons
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            fetch_button = st.button(
                "🔄 Fetch Proposal Data",
                type="primary",
                disabled=len(subunits) == 0,
                use_container_width=True
            )
        
        with col2:
            if st.session_state.processed_data is not None:
                st.success("✅ Data ready - Report shown below")
            else:
                st.info("📊 Report will appear after data is fetched")
        
        with col3:
            if len(subunits) == 0:
                st.warning("⚠️ Select at least one sub-DAO to fetch proposal data")
    else:
        st.info("No sub-DAOs found. Please check the main DAO address in the sidebar.")
    
    # Fetch data when button is clicked
    if fetch_button:
        with st.spinner("Fetching proposal data..."):
            api_client = DAOAPIClient(api_base_url, network)
            st.session_state.proposal_data = {}
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, (name, address) in enumerate(subunits.items()):
                status_text.text(f"Fetching data for {name}...")
                try:
                    proposals = api_client.get_proposals(address)
                    st.session_state.proposal_data[name] = {
                        'address': address,
                        'proposals': proposals
                    }
                    st.success(f"✅ Fetched {len(proposals)} proposals for {name}")
                except Exception as e:
                    st.error(f"❌ Error fetching data for {name}: {str(e)}")
                    st.session_state.proposal_data[name] = {
                        'address': address,
                        'proposals': [],
                        'error': str(e)
                    }
                
                progress_bar.progress((i + 1) / len(subunits))
            
            status_text.text("Processing data...")
            
            # Process the fetched data
            if st.session_state.proposal_data:
                processor = DataProcessor(core_team_addresses, st.session_state.token_data)
                st.session_state.processed_data = processor.process_all_proposals(st.session_state.proposal_data)
                st.success("✅ Data processing completed!")
            
            status_text.empty()
            progress_bar.empty()
    
    # Display data summary
    if st.session_state.proposal_data:
        st.subheader("Data Summary")
        
        summary_data = []
        for name, data in st.session_state.proposal_data.items():
            if 'error' in data:
                summary_data.append({
                    'Sub-unit': name,
                    'Status': '❌ Error',
                    'Proposals': 0,
                    'Details': data['error']
                })
            else:
                summary_data.append({
                    'Sub-unit': name,
                    'Status': '✅ Success',
                    'Proposals': len(data['proposals']),
                    'Details': f"{len(data['proposals'])} proposals fetched"
                })
        
        df_summary = pd.DataFrame(summary_data)
        st.dataframe(df_summary, use_container_width=True)

        # Debug: show a raw proposal sample per sub-unit to inspect message shape
        if st.session_state.get('debug', False):
            st.subheader("Raw proposal samples (debug)")
            for name, data in st.session_state.proposal_data.items():
                proposals = data.get('proposals', []) if isinstance(data, dict) else []
                with st.expander(f"{name} - sample proposal (expand to view)"):
                    if proposals:
                        try:
                            # Show the first proposal JSON for inspection
                            st.json(proposals[0])
                        except Exception:
                            st.write(proposals[0])
                    else:
                        st.info("No proposals fetched for this sub-unit")

            # Show processing diagnostics if available
            diagnostics = st.session_state.get('processing_diagnostics')
            if diagnostics:
                st.subheader("Processing diagnostics")
                st.write(diagnostics)
    
    # Generate and display report automatically when data is available
    if st.session_state.processed_data is not None and not st.session_state.processed_data.empty:
        st.subheader("📊 Accounting Report")
        
        report_generator = ReportGenerator()
        
        # Generate summary statistics and insights
        summary_stats = report_generator.generate_summary_stats(st.session_state.processed_data)
        insights = report_generator.generate_transaction_insights(st.session_state.processed_data)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Payments", f"{summary_stats['total_payments']:,}")
        with col2:
            # Display USD value if available
            if 'total_usd_value' in summary_stats and summary_stats['total_usd_value'] > 0:
                st.metric("Total USD Value", f"${summary_stats['total_usd_value']:,.2f}")
            else:
                total_display = f"{summary_stats['total_amount_osmo']:,.2f} (mixed tokens)"
                st.metric("Total Amount", total_display)
        with col3:
            core_team_pct = insights.get('core_team_percentage', 0)
            st.metric("Core Team Payments", f"{summary_stats['core_team_payments']:,}", 
                     delta=f"{core_team_pct:.1f}% of total value")
        with col4:
            st.metric("Sub-DAOs", f"{summary_stats['subunits_count']:,}")
        
        # Key Insights Section
        if insights:
            st.subheader("🔍 Key Insights")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if 'largest_transaction' in insights:
                    largest = insights['largest_transaction']
                    # Get token symbol for largest transaction
                    largest_symbol = largest.get('symbol', 'tokens')
                    # Prefer USD value display when available
                    usd_display = largest.get('usd_value')
                    if usd_display is not None:
                        st.info(f"**Largest Transaction:** ${usd_display:,.2f} to {largest['recipient'][:20]}... ({largest['category']})")
                    else:
                        st.info(f"**Largest Transaction:** {largest['amount']:,.2f} {largest_symbol} to {largest['recipient'][:20]}... ({largest['category']})")
            
            with col2:
                if 'most_frequent_recipient' in insights:
                    frequent = insights['most_frequent_recipient']
                    total_usd = frequent.get('total_usd')
                    if total_usd is not None:
                        st.info(f"**Most Frequent Recipient:** {frequent['count']} transactions totaling ${total_usd:,.2f}")
                    else:
                        # Fallback to count-only message but use 'transactions' wording
                        st.info(f"**Most Frequent Recipient:** {frequent['count']} transactions")
        
        # Payments by sub-unit
        st.subheader("Payments by Sub-unit (USD)")
        subunit_summary = report_generator.generate_subunit_summary(st.session_state.processed_data)
        if subunit_summary is None:
            subunit_summary = pd.DataFrame()

        if not subunit_summary.empty:
            st.dataframe(subunit_summary, use_container_width=True)
            if HAS_PLOTLY:
                try:
                    fig_pie = px.pie(
                        subunit_summary,
                        values='Total USD',
                        names='Sub-unit',
                        title="Payment Distribution by Sub-unit (USD)"
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                except Exception as e:
                    st.warning(f"Could not render Plotly chart: {e}")
            else:
                st.info("Plotly not installed — charts are disabled. Install plotly+kaleido to enable charts.")

        # Enhanced analysis sections
        tab1, tab2, tab3, tab4 = st.tabs(["💰 Payment Categories", "📊 Amount Ranges", "👥 Core Team Analysis", "🏷️ Transaction Tags"])

        with tab1:
            st.subheader("Transaction Categories (USD)")
            category_breakdown = report_generator.generate_category_breakdown(st.session_state.processed_data)
            if not category_breakdown.empty:
                col1, col2 = st.columns(2)
                with col1:
                    st.dataframe(category_breakdown, use_container_width=True)
                with col2:
                    if HAS_PLOTLY:
                        try:
                            fig_category = px.pie(
                                category_breakdown,
                                values='Total USD',
                                names='Transaction Category',
                                title="Spending by Transaction Category (USD)"
                            )
                            st.plotly_chart(fig_category, use_container_width=True)
                        except Exception as e:
                            st.warning(f"Could not render Plotly chart: {e}")
                    else:
                        st.info("Plotly not installed — charts disabled.")

        with tab2:
            st.subheader("Amount Range Analysis (USD)")
            amount_analysis = report_generator.generate_amount_range_analysis(st.session_state.processed_data)
            if not amount_analysis.empty:
                col1, col2 = st.columns(2)
                with col1:
                    st.dataframe(amount_analysis, use_container_width=True)
                with col2:
                    if HAS_PLOTLY:
                        try:
                            fig_amounts = px.bar(
                                amount_analysis,
                                x='Amount Category',
                                y='Total USD',
                                title="Spending by Amount Range (USD)",
                                text='Number of Transactions'
                            )
                            fig_amounts.update_xaxes(tickangle=45)
                            st.plotly_chart(fig_amounts, use_container_width=True)
                        except Exception as e:
                            st.warning(f"Could not render Plotly chart: {e}")
                    else:
                        st.info("Plotly not installed — charts disabled.")

        with tab3:
            if summary_stats['core_team_payments'] > 0:
                st.subheader("Core Team vs Non-Core Team Analysis (USD)")
                core_breakdown = report_generator.generate_core_team_breakdown(st.session_state.processed_data)
                col1, col2 = st.columns(2)
                with col1:
                    st.dataframe(core_breakdown, use_container_width=True)
                with col2:
                    if HAS_PLOTLY:
                        try:
                            fig_bar = px.bar(
                                core_breakdown,
                                x='Type',
                                y='Total USD',
                                title="Core Team vs Non-Core Team Payments (USD)",
                                color='Type',
                                text='Number of Payments'
                            )
                            st.plotly_chart(fig_bar, use_container_width=True)
                        except Exception as e:
                            st.warning(f"Could not render Plotly chart: {e}")
                    else:
                        st.info("Plotly not installed — charts disabled.")
            else:
                st.info("No core team members configured. Add core team addresses in the sidebar to see this analysis.")

        with tab4:
            if 'Transaction Tag' in st.session_state.processed_data.columns:
                st.subheader("Transaction Tags Analysis")

                # Expand tags and analyze
                tags_data = []
                for _, row in st.session_state.processed_data.iterrows():
                    tag_value = row['Transaction Tag']
                    if isinstance(tag_value, str):
                        tags = tag_value.split(' | ')
                    else:
                        tags = [str(tag_value)]
                    for tag in tags:
                        tags_data.append({
                            'Tag': tag,
                            'USD Value': row['USD Value'],
                            'Sub-unit': row['Sub-unit']
                        })

                if tags_data:
                    tags_df = pd.DataFrame(tags_data)
                    tag_summary = tags_df.groupby('Tag').agg({
                        'USD Value': ['sum', 'count']
                    }).round(2)
                    tag_summary.columns = ['Total USD', 'Count']
                    tag_summary = tag_summary.reset_index().sort_values('Total USD', ascending=False)
                    col1, col2 = st.columns(2)
                    with col1:
                        st.dataframe(tag_summary, use_container_width=True)
                    with col2:
                        if HAS_PLOTLY:
                            try:
                                fig_tags = px.bar(
                                    tag_summary.head(10),
                                    x='Tag',
                                    y='Total USD',
                                    title="Top 10 Transaction Tags by USD Value"
                                )
                                fig_tags.update_xaxes(tickangle=45)
                                st.plotly_chart(fig_tags, use_container_width=True)
                            except Exception as e:
                                st.warning(f"Could not render Plotly chart: {e}")
                        else:
                            st.info("Plotly not installed — charts disabled.")
            
            # Detailed transactions table
            st.subheader("Detailed Transactions")

            detailed_transactions = report_generator.generate_detailed_report(st.session_state.processed_data, include_zero_usd=st.session_state.include_zero_usd)

            if not detailed_transactions.empty:
                # Add filters
                col1, col2, col3 = st.columns(3)

                with col1:
                    selected_org_units = st.multiselect(
                        "Filter by Org Unit",
                        options=detailed_transactions['Org Unit'].unique(),
                        default=detailed_transactions['Org Unit'].unique()
                    )

                with col2:
                    if 'Transaction Category' in detailed_transactions.columns:
                        selected_categories = st.multiselect(
                            "Filter by Transaction Category",
                            options=detailed_transactions['Transaction Category'].unique(),
                            default=detailed_transactions['Transaction Category'].unique()
                        )
                    else:
                        selected_categories = []

                with col3:
                    min_amount = st.number_input(
                        "Minimum Amount",
                        min_value=0.0,
                        value=0.0,
                        step=1.0
                    )

                # Apply filters (use USD Value for amount filtering)
                filter_conditions = (
                    (detailed_transactions['Org Unit'].isin(selected_org_units)) &
                    (detailed_transactions['USD Value'] >= min_amount)
                )

                if selected_categories:
                    filter_conditions &= detailed_transactions['Transaction Category'].isin(selected_categories)

                filtered_transactions = detailed_transactions[filter_conditions]

                st.dataframe(filtered_transactions, use_container_width=True)

                # Export functionality
                st.subheader("Export Options")

                col1, col2 = st.columns(2)

                with col1:
                    csv_data = filtered_transactions.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv_data,
                        file_name=f"dao_accounting_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )

                with col2:
                    json_data = filtered_transactions.to_json(orient='records', indent=2)
                    if json_data:
                        st.download_button(
                            label="📥 Download as JSON",
                            data=json_data,
                            file_name=f"dao_accounting_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )

                    # PDF export: filtered and full (cleanly separated)
                    # Filtered PDF
                    if st.button("🖨️ Download PDF (filtered view)"):
                        try:
                            pdf_bytes = report_generator.export_to_pdf(
                                processed_data=st.session_state.processed_data,
                                detailed_df=filtered_transactions,
                                title=f"DAO Accounting Report - Filtered {datetime.now().strftime('%Y-%m-%d')}",
                                include_zero_usd=st.session_state.include_zero_usd,
                                subdaos=list(subunits.keys()) if subunits else None,
                                main_dao=st.session_state.main_dao_address or None,
                                core_team=core_team_addresses,
                                proposals_count=sum([len(v.get('proposals', [])) for v in st.session_state.proposal_data.values()])
                            )
                            if pdf_bytes:
                                st.download_button(
                                    label="Download PDF (filtered)",
                                    data=pdf_bytes,
                                    file_name=f"dao_accounting_report_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                                    mime='application/pdf'
                                )
                            else:
                                st.warning("No data available to export to PDF (filtered)")
                        except Exception as e:
                            st.error(f"PDF export failed: {e}")

                    # Full PDF
                    if st.button("🖨️ Download PDF (full report)"):
                        try:
                            # Build the detailed DataFrame according to the user's include_zero_usd setting and pass it to the exporter
                            detailed_for_pdf = report_generator.generate_detailed_report(
                                st.session_state.processed_data,
                                include_zero_usd=st.session_state.include_zero_usd
                            )
                            pdf_bytes = report_generator.export_to_pdf(
                                processed_data=st.session_state.processed_data,
                                detailed_df=detailed_for_pdf,
                                title=f"DAO Accounting Report - Full {datetime.now().strftime('%Y-%m-%d')}",
                                include_zero_usd=st.session_state.include_zero_usd,
                                subdaos=list(subunits.keys()) if subunits else None,
                                main_dao=st.session_state.main_dao_address or None,
                                core_team=core_team_addresses,
                                proposals_count=sum([len(v.get('proposals', [])) for v in st.session_state.proposal_data.values()])
                            )
                            if pdf_bytes:
                                st.download_button(
                                    label="Download PDF (full)",
                                    data=pdf_bytes,
                                    file_name=f"dao_accounting_report_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                                    mime='application/pdf'
                                )
                            else:
                                st.warning("No data available to export to PDF (full)")
                        except Exception as e:
                            st.error(f"PDF export failed: {e}")
            else:
                st.info("No transaction data available to display.")

    else:
        st.info("No payment data found in the fetched proposals.")

if __name__ == "__main__":
    main()
