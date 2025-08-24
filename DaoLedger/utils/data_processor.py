import json
import base64
from typing import List, Dict, Any, Optional
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

class DataProcessor:
    """Process DAO proposal data to extract payment information"""
    
    def __init__(self, core_team_addresses: Optional[List[str]] = None, token_data: Optional[dict] = None, pricing_data: Optional[dict] = None):
        self.core_team_addresses = set(core_team_addresses or [])
        self.token_data = token_data or {}
        self.pricing_data = pricing_data or {}
        self._load_pricing_data()
    
    def process_all_proposals(self, proposal_data: Dict[str, Any]) -> pd.DataFrame:
        """
        Process all proposal data from multiple sub-units
        
        Args:
            proposal_data: Dictionary containing proposal data for each sub-unit
            
        Returns:
            DataFrame with processed payment information
        """
        all_payments = []
        diagnostics = {
            'subunits': {},
            'total_messages_scanned': 0,
            'total_payments_extracted': 0
        }
        
        for subunit_name, data in proposal_data.items():
            if 'error' in data:
                continue
                
            proposals = data.get('proposals', [])
            subunit_address = data.get('address', '')
            
            for proposal in proposals:
                payments = self.extract_payments_from_proposal(proposal, subunit_name, subunit_address)
                all_payments.extend(payments)
                # Count messages scanned in this proposal for diagnostics
                try:
                    proposal_obj = proposal.get('proposal') if isinstance(proposal, dict) and 'proposal' in proposal else proposal
                    msgs = []
                    if isinstance(proposal_obj, dict):
                        msgs = proposal_obj.get('msgs') or proposal_obj.get('messages') or []
                    if not msgs and isinstance(proposal, dict):
                        msgs = proposal.get('msgs') or proposal.get('messages') or []
                    if isinstance(msgs, dict):
                        msgs = [msgs]
                    diagnostics['total_messages_scanned'] += len(msgs) if isinstance(msgs, list) else 0
                except Exception:
                    pass
        
        if not all_payments:
            # Save diagnostics to session state for UI inspection
            try:
                diagnostics['total_payments_extracted'] = 0
                st.session_state['processing_diagnostics'] = diagnostics
            except Exception:
                pass
            return pd.DataFrame()
        
        df = pd.DataFrame(all_payments)
        
        # Adjust amounts using token decimals and update display symbols
        df['Adjusted Amount'] = df.apply(self._adjust_amount, axis=1)
        df['Display Symbol'] = df['Denom'].apply(self._get_token_symbol)
        
        # Add derived columns
        df['Payment Type'] = df['Recipient'].apply(
            lambda x: 'Core Team' if x in self.core_team_addresses else 'Regular'
        )
        
        # Calculate USD values
        df['USD Value'] = df.apply(self._calculate_usd_value, axis=1)
        
        # Enhanced transaction categorization
        df['Transaction Category'] = df.apply(self._categorize_transaction, axis=1)
        df['Transaction Tag'] = df.apply(self._tag_transaction, axis=1)
        df['Amount Category'] = df.apply(self._categorize_amount, axis=1)
        df['Recipient Type'] = df['Recipient'].apply(self._classify_recipient)

        # Save diagnostics: total payments extracted
        try:
            diagnostics['total_payments_extracted'] = len(df)
            st.session_state['processing_diagnostics'] = diagnostics
        except Exception:
            pass
        
        return df
    
    def extract_payments_from_proposal(self, proposal: Dict[Any, Any], subunit_name: str, subunit_address: str) -> List[Dict[str, Any]]:
        """
        Extract payment information from a single proposal
        
        Args:
            proposal: Proposal data
            subunit_name: Name of the sub-unit
            subunit_address: Contract address of the sub-unit
            
        Returns:
            List of payment dictionaries
        """
        payments = []

        try:
            # Try several common keys for proposal id
            proposal_id = proposal.get('id') or proposal.get('proposal_id') or proposal.get('proposalId') or 'Unknown'

            # Proposal payload may be nested under 'proposal' or be the object itself
            if isinstance(proposal.get('proposal'), dict):
                proposal_obj = proposal.get('proposal')
            else:
                proposal_obj = proposal

            # Extract proposal text content if present
            proposal_title = proposal_obj.get('title', '') if isinstance(proposal_obj, dict) else ''
            proposal_description = proposal_obj.get('description', '') if isinstance(proposal_obj, dict) else ''
            proposal_text = f"{proposal_title}\n{proposal_description}".strip()

            # Extract proposal date for pricing lookup (some APIs put date at top-level or inside proposal)
            proposal_date = self._extract_proposal_date(proposal)

            # Support multiple possible message keys: 'msgs', 'messages', 'msgs' inside nested fields
            msgs = []
            if isinstance(proposal_obj, dict):
                msgs = proposal_obj.get('msgs') or proposal_obj.get('messages') or proposal_obj.get('msgs', [])
            # Fallback to top-level
            if not msgs and isinstance(proposal, dict):
                msgs = proposal.get('msgs') or proposal.get('messages') or []

            # Ensure msgs is a list
            if isinstance(msgs, dict):
                msgs = [msgs]
            if not isinstance(msgs, list):
                msgs = []

            # Iterate messages and dispatch to processors
            for msg in msgs:
                if not isinstance(msg, dict):
                    continue
                # Some APIs wrap message under 'msg' key
                inner = None
                # If message already formatted like {'wasm': {...}}, use it directly
                if any(k in msg for k in ['wasm', 'bank', 'stargate']):
                    inner = msg
                else:
                    # Try common wrappers
                    inner = msg.get('msg') or msg.get('value') or msg

                # Dispatch by detected keys
                if isinstance(inner, dict) and 'stargate' in inner:
                    payments.extend(self.process_stargate_message(inner['stargate'], proposal_id, subunit_name, subunit_address, proposal_text, proposal_date, proposal_title=proposal_title))
                elif isinstance(inner, dict) and 'bank' in inner:
                    payments.extend(self.process_bank_message(inner['bank'], proposal_id, subunit_name, subunit_address, proposal_text, proposal_date, proposal_title=proposal_title))
                elif isinstance(inner, dict) and 'wasm' in inner:
                    payments.extend(self.process_wasm_message(inner['wasm'], proposal_id, subunit_name, subunit_address, proposal_text, proposal_date, proposal_title=proposal_title))
                else:
                    # Try generic extraction
                    payments.extend(self.process_other_message(inner if isinstance(inner, dict) else msg, proposal_id, subunit_name, subunit_address, proposal_text, proposal_date, proposal_title=proposal_title))

        except Exception as e:
            # Use safer warning; avoid crashing the whole run
            if st.session_state.get('debug', False):
                st.warning(f"Error processing proposal {proposal_id}: {str(e)}")

        return payments
    
    def process_stargate_message(self, stargate_msg: Dict[str, Any], proposal_id: Any, subunit_name: str, subunit_address: str, proposal_text: str = '', proposal_date: str = '', proposal_title: str = '') -> List[Dict[str, Any]]:
        """Process stargate messages to extract payment information"""
        payments = []
        
        try:
            # Decode base64 value if present
            value = stargate_msg.get('value', '')
            if value:
                try:
                    # Try to decode base64
                    decoded_bytes = base64.b64decode(value)
                    # This would require protobuf decoding for cosmos messages
                    # For now, we'll extract what we can from the raw data
                    
                    # Look for address patterns and amounts in the decoded data
                    decoded_str = decoded_bytes.decode('utf-8', errors='ignore')
                    
                    # Extract potential addresses (osmo1... pattern)
                    import re
                    addresses = re.findall(r'osmo1[a-z0-9]{38,58}', decoded_str)
                    amounts = re.findall(r'\d+uosmo', decoded_str)
                    
                    if addresses and amounts:
                        for addr in addresses:
                            if addr != subunit_address:  # Don't include the source address
                                payments.append({
                                    'Proposal ID': proposal_id,
                                    'Proposal Title': proposal_title,
                                    'Proposal Text': proposal_text,
                                    'Proposal Date': proposal_date,
                                    'Sub-unit': subunit_name,
                                    'Sub-unit Address': subunit_address,
                                    'Recipient': addr,
                                    'Amount (uosmo)': amounts[0] if amounts else '0',
                                    'Amount (Raw)': float(amounts[0].replace('uosmo', '')) if amounts else 0,
                                    'Amount (OSMO)': float(amounts[0].replace('uosmo', '')) / 1000000 if amounts else 0.0,
                                    'Denom': 'uosmo',
                                    'Message Type': 'stargate'
                                })
                except Exception as e:
                    # If decoding fails, try to extract from the original structure
                    pass
                    
        except Exception as e:
            st.warning(f"Error processing stargate message: {str(e)}")
        
        return payments
    
    def process_bank_message(self, bank_msg: Dict[str, Any], proposal_id: Any, subunit_name: str, subunit_address: str, proposal_text: str = '', proposal_date: str = '', proposal_title: str = '') -> List[Dict[str, Any]]:
        """Process bank send messages"""
        payments = []
        
        try:
            if 'send' in bank_msg:
                send_msg = bank_msg['send']
                to_address = send_msg.get('to_address', '')
                amount = send_msg.get('amount', [])
                
                for coin in amount:
                    denom = coin.get('denom', '')
                    amount_value = int(coin.get('amount', '0'))
                    
                    # Convert uosmo to OSMO
                    if denom == 'uosmo':
                        osmo_amount = amount_value / 1000000
                    else:
                        osmo_amount = amount_value
                    
                    payments.append({
                        'Proposal ID': proposal_id,
                        'Proposal Title': proposal_title,
                        'Proposal Text': proposal_text,
                        'Proposal Date': proposal_date,
                        'Sub-unit': subunit_name,
                        'Sub-unit Address': subunit_address,
                        'Recipient': to_address,
                        'Amount (uosmo)': str(amount_value),
                        'Amount (Raw)': amount_value,
                        'Amount (OSMO)': osmo_amount,
                        'Denom': denom,
                        'Message Type': 'bank_send'
                    })
                    
        except Exception as e:
            st.warning(f"Error processing bank message: {str(e)}")
        
        return payments
    
    def process_wasm_message(self, wasm_msg: Dict[str, Any], proposal_id: Any, subunit_name: str, subunit_address: str, proposal_text: str = '', proposal_date: str = '', proposal_title: str = '') -> List[Dict[str, Any]]:
        """Process wasm execute messages, including vesting/payroll contracts, and ensure one row per recipient/amount pair."""
        payments = []
        try:
            if 'execute' in wasm_msg:
                execute_msg = wasm_msg['execute']
                contract = execute_msg.get('contract', '') or execute_msg.get('contract_addr', '')
                msg = execute_msg.get('msg', {})
                funds = execute_msg.get('funds', [])

                # Try to decode base64 encoded message
                decoded_msg = self._decode_wasm_message(msg)


                # Handle vesting/payroll contract instantiation
                for key, value in decoded_msg.items():
                    if key == 'instantiate_native_payroll_contract' and isinstance(value, dict):
                        inst_msg = value.get('instantiate_msg', {})
                        recipient = inst_msg.get('recipient')
                        total = inst_msg.get('total')
                        denom = inst_msg.get('denom', {}).get('native', 'token')
                        if recipient and total:
                            payments.append({
                                'Proposal ID': proposal_id,
                                'Proposal Title': proposal_title,
                                'Proposal Text': proposal_text,
                                'Proposal Date': proposal_date,
                                'Sub-unit': subunit_name,
                                'Sub-unit Address': subunit_address,
                                'Recipient': recipient,
                                'Amount (uosmo)': str(total),
                                'Amount (Raw)': float(total) if str(total).isdigit() else 0,
                                'Amount (OSMO)': float(total) / 1000000 if str(total).isdigit() else 0.0,
                                'Denom': denom,
                                'Message Type': 'wasm_instantiate_native_payroll_contract',
                                'Contract Method': key,
                                'Contract Address': contract
                            })
                    # Handle staking messages
                    if key == 'stake' and isinstance(value, dict):
                        for fund in funds:
                            denom = fund.get('denom', '')
                            amount_value = int(fund.get('amount', '0'))
                            if denom == 'uosmo':
                                osmo_amount = amount_value / 1000000
                            else:
                                osmo_amount = amount_value
                            payments.append({
                                'Proposal ID': proposal_id,
                                'Proposal Title': proposal_title,
                                'Proposal Text': proposal_text,
                                'Proposal Date': proposal_date,
                                'Sub-unit': subunit_name,
                                'Sub-unit Address': subunit_address,
                                'Recipient': contract,
                                'Amount (uosmo)': str(amount_value),
                                'Amount (Raw)': amount_value,
                                'Amount (OSMO)': osmo_amount,
                                'Denom': denom,
                                'Message Type': 'wasm_stake',
                                'Contract Method': key,
                                'Contract Address': contract,
                                'Transaction Category': 'Staking'
                            })

                # Handle transfer/send fields (one row per recipient)
                for transfer_type in ['transfer', 'send']:
                    if transfer_type in decoded_msg:
                        rec = decoded_msg[transfer_type].get('recipient')
                        amt = decoded_msg[transfer_type].get('amount')
                        if rec and amt:
                            payments.append({
                                'Proposal ID': proposal_id,
                                'Proposal Title': proposal_title,
                                'Proposal Text': proposal_text,
                                'Proposal Date': proposal_date,
                                'Sub-unit': subunit_name,
                                'Sub-unit Address': subunit_address,
                                'Recipient': rec,
                                'Amount (uosmo)': str(amt),
                                'Amount (Raw)': float(amt) if str(amt).isdigit() else 0,
                                'Amount (OSMO)': float(amt) / 1000000 if str(amt).isdigit() else 0.0,
                                'Denom': 'token',
                                'Message Type': f'wasm_{transfer_type}',
                                'Contract Method': transfer_type,
                                'Contract Address': contract
                            })

                # Parse complex contract instantiation/execution messages
                contract_data = self._parse_contract_execution(decoded_msg, contract, proposal_id, proposal_text, subunit_name, subunit_address)
                payments.extend(contract_data)

                # Also check funds sent with the message, but avoid duplicate contract/recipient rows
                for fund in funds:
                    denom = fund.get('denom', '')
                    amount_value = int(fund.get('amount', '0'))
                    if denom == 'uosmo':
                        osmo_amount = amount_value / 1000000
                    else:
                        osmo_amount = amount_value
                    payments.append({
                        'Proposal ID': proposal_id,
                        'Proposal Title': proposal_title,
                        'Proposal Text': proposal_text,
                        'Proposal Date': proposal_date,
                        'Sub-unit': subunit_name,
                        'Sub-unit Address': subunit_address,
                        'Recipient': contract,
                        'Amount (uosmo)': str(amount_value),
                        'Amount (Raw)': amount_value,
                        'Amount (OSMO)': osmo_amount,
                        'Denom': denom,
                        'Message Type': 'wasm_execute_funds',
                        'Contract Method': self._extract_method_name(decoded_msg),
                        'Contract Address': contract
                    })
        except Exception as e:
            st.warning(f"Error processing wasm message: {str(e)}")
        return payments
    
    def process_other_message(self, msg: Dict[str, Any], proposal_id: Any, subunit_name: str, subunit_address: str, proposal_text: str = '', proposal_date: str = '', proposal_title: str = '') -> List[Dict[str, Any]]:
        """Process other message types to extract any payment information"""
        payments = []
        
        try:
            # Look for common payment patterns in any message type
            msg_str = json.dumps(msg)
            
            # Extract addresses and amounts using regex
            import re
            addresses = re.findall(r'osmo1[a-z0-9]{38,58}', msg_str)
            amounts = re.findall(r'(\d+)uosmo', msg_str)
            
            # Remove the source address from recipients
            recipient_addresses = [addr for addr in addresses if addr != subunit_address]

            if recipient_addresses and amounts:
                for addr in recipient_addresses:
                    amount_value = int(amounts[0]) if amounts else 0
                    osmo_amount = amount_value / 1000000

                    payments.append({
                        'Proposal ID': proposal_id,
                        'Proposal Title': proposal_title,
                        'Proposal Text': proposal_text,
                        'Proposal Date': proposal_date,
                        'Sub-unit': subunit_name,
                        'Sub-unit Address': subunit_address,
                        'Recipient': addr,
                        'Amount (uosmo)': str(amount_value),
                        'Amount (Raw)': amount_value,
                        'Amount (OSMO)': osmo_amount,
                        'Denom': 'uosmo',
                        'Message Type': 'other'
                    })
                    
        except Exception as e:
            # Ignore errors in other message processing
            pass
        
        return payments
    
    def _categorize_transaction(self, row) -> str:
        """
        Categorize transactions based on amount, recipient, and message type
        """
        amount = row.get('Adjusted Amount', row.get('Amount (OSMO)', 0))
        message_type = row.get('Message Type', '')
        payment_type = row.get('Payment Type', '')
        
        # Large amount transactions (over 10,000 OSMO)
        if amount >= 10000:
            if payment_type == 'Core Team':
                return 'Large Core Team Payment'
            else:
                return 'Large External Payment'
        
        # Medium amount transactions (1,000 - 10,000 OSMO)
        elif amount >= 1000:
            if payment_type == 'Core Team':
                return 'Medium Core Team Payment'
            else:
                return 'Medium External Payment'
        
        # Small amount transactions (100 - 1,000 OSMO)
        elif amount >= 100:
            if payment_type == 'Core Team':
                return 'Small Core Team Payment'
            else:
                return 'Small External Payment'
        
        # Micro transactions (under 100 OSMO)
        else:
            return 'Micro Payment'
    
    def _tag_transaction(self, row) -> str:
        """
        Add intelligent tags based on transaction characteristics
        """
        message_type = row.get('Message Type', '')
        amount = row.get('Adjusted Amount', row.get('Amount (OSMO)', 0))
        recipient = row.get('Recipient', '')
        
        tags = []
        
        # Message type tags
        if 'bank' in message_type:
            tags.append('Direct Transfer')
        elif 'stargate' in message_type:
            tags.append('Protocol Operation')
        elif 'wasm' in message_type:
            tags.append('Smart Contract')
        
        # Amount-based tags
        if amount >= 50000:
            tags.append('Major Expenditure')
        elif amount >= 10000:
            tags.append('Significant Payment')
        elif amount >= 1000:
            tags.append('Standard Payment')
        elif amount >= 100:
            tags.append('Minor Payment')
        else:
            tags.append('Micro Payment')
        
        # Recipient pattern analysis
        if recipient.startswith('osmo1'):
            # Check for common patterns
            if len(recipient) > 50:
                tags.append('Contract Address')
            else:
                tags.append('Wallet Address')
        
        # Frequency analysis (if multiple payments to same recipient)
        # This would require additional context, but we can add basic logic
        
        return ' | '.join(tags) if tags else 'Standard'
    
    def _categorize_amount(self, row) -> str:
        """
        Categorize amounts into ranges for reporting
        """
        amount = row.get('Adjusted Amount', row.get('Amount (OSMO)', 0))
        
        if amount >= 100000:
            return 'Very Large (100K+ OSMO)'
        elif amount >= 50000:
            return 'Large (50K-100K OSMO)'
        elif amount >= 10000:
            return 'Medium (10K-50K OSMO)'
        elif amount >= 1000:
            return 'Small (1K-10K OSMO)'
        elif amount >= 100:
            return 'Minor (100-1K OSMO)'
        else:
            return 'Micro (<100 OSMO)'
    
    def _adjust_amount(self, row) -> float:
        """
        Adjust raw amount using token decimals
        """
        raw_amount = row.get('Amount (Raw)', 0)
        denom = row.get('Denom', '')
        
        # Look up token info
        token_info = self.token_data.get(denom, {})
        decimals = token_info.get('decimals', 0)
        
        # Adjust amount by dividing by 10^decimals
        if decimals > 0:
            adjusted = raw_amount / (10 ** decimals)
        else:
            adjusted = raw_amount
        
        return adjusted
    
    def _get_token_symbol(self, denom: str) -> str:
        """
        Get display symbol for a token denomination
        """
        if not denom or not isinstance(denom, str):
            return ''

        # Direct lookup in provided token_data
        token_info = self.token_data.get(denom)
        if token_info and isinstance(token_info, dict):
            sym = token_info.get('symbol')
            if sym:
                return sym

        # Normalize denom and try common patterns
        d = denom.strip()
        dl = d.lower()

        # Common on-chain microdenoms
        if dl in ('uosmo', 'uosmosis'):
            return 'OSMO'
        # Secret (SCRT) common patterns
        if 'scrt' in dl or 'secret' in dl:
            return 'SCRT'

        # If denom is an IBC denom, sometimes the display symbol is stored elsewhere; try to match known token_data values
        try:
            for k, info in (self.token_data or {}).items():
                if not isinstance(info, dict):
                    continue
                sym = (info.get('symbol') or '').upper()
                if not sym:
                    continue
                # If the denom string contains the symbol or the info key matches, prefer that symbol
                if sym in d.upper() or k.upper() in d.upper():
                    return sym
        except Exception:
            pass

        # Fallback: return the denom itself (uppercased for tickers when possible)
        # If denom looks like a ticker already, return uppercased
        if len(d) <= 8 and d.isalpha():
            return d.upper()

        return d
    
    def _classify_recipient(self, recipient: str) -> str:
        """
        Classify recipient address as contract or wallet
        """
        if not recipient or not isinstance(recipient, str):
            return 'Unknown'
        
        # Basic classification based on address characteristics
        if recipient.startswith('osmo1'):
            # Contract addresses are typically longer and have specific patterns
            if len(recipient) > 50:
                return 'Smart Contract'
            elif len(recipient) == 63:  # Standard bech32 address length
                return 'Wallet Address'
            elif len(recipient) > 63:
                return 'Smart Contract'
            else:
                return 'Wallet Address'
        else:
            return 'Other Address'
    
    def _decode_wasm_message(self, msg: Any) -> Dict[str, Any]:
        """
        Decode base64-encoded WASM message if it's a string
        """
        if isinstance(msg, str):
            try:
                # Try to decode as base64 UTF-8
                decoded_bytes = base64.b64decode(msg)
                decoded_str = decoded_bytes.decode('utf-8')
                return json.loads(decoded_str)
            except (Exception, UnicodeDecodeError, json.JSONDecodeError):
                # If decoding fails, return empty dict
                return {}
        elif isinstance(msg, dict):
            # Already decoded
            return msg
        else:
            return {}
    
    def _extract_method_name(self, decoded_msg: Dict[str, Any]) -> str:
        """
        Extract the method name from decoded WASM message
        """
        if not decoded_msg:
            return ''
        
        # The method name is typically the first key in the message
        for key in decoded_msg.keys():
            return key
        return ''
    
    def _parse_contract_execution(self, decoded_msg: Dict[str, Any], contract: str, proposal_id: Any, 
                                 proposal_text: str, subunit_name: str, subunit_address: str) -> List[Dict[str, Any]]:
        """
        Parse complex contract execution messages to extract payment data
        """
        payments = []
        
        try:
            method_name = self._extract_method_name(decoded_msg)
            method_data = decoded_msg.get(method_name, {})
            
            # Handle instantiate_native_payroll_contract
            if 'instantiate_native_payroll_contract' in decoded_msg:
                instantiate_data = method_data.get('instantiate_msg', {})
                recipient = instantiate_data.get('recipient', '')
                total_amount = instantiate_data.get('total', '0')
                title = instantiate_data.get('title', '')
                owner = instantiate_data.get('owner', '')
                denom_info = instantiate_data.get('denom', {})
                
                # Extract denomination
                if isinstance(denom_info, dict):
                    if 'native' in denom_info:
                        denom = denom_info['native']
                    else:
                        denom = str(denom_info)
                else:
                    denom = str(denom_info)
                
                if recipient and total_amount and str(total_amount).isdigit():
                    amount_value = int(total_amount)
                    
                    payments.append({
                        'Proposal ID': proposal_id,
                        'Proposal Text': proposal_text,
                        'Sub-unit': subunit_name,
                        'Sub-unit Address': subunit_address,
                        'Recipient': recipient,
                        'Amount (uosmo)': str(amount_value),
                        'Amount (Raw)': amount_value,
                        'Amount (OSMO)': amount_value / 1000000 if 'uosmo' in denom else amount_value,
                        'Denom': denom,
                        'Message Type': 'contract_instantiate',
                        'Contract Method': method_name,
                        'Contract Address': contract,
                        'Contract Title': title,
                        'Contract Owner': owner
                    })
            
            # Look for other payment patterns in any method
            self._extract_recursive_payments(method_data, payments, {
                'Proposal ID': proposal_id,
                'Proposal Text': proposal_text,
                'Sub-unit': subunit_name,
                'Sub-unit Address': subunit_address,
                'Message Type': 'wasm_contract_call',
                'Contract Method': method_name,
                'Contract Address': contract
            })
            
        except Exception as e:
            # Don't show errors for failed contract parsing
            pass
        
        return payments
    
    def _extract_recursive_payments(self, data: Any, payments: List[Dict[str, Any]], base_payment: Dict[str, Any]):
        """
        Recursively extract payment information from nested contract data
        """
        if isinstance(data, dict):
            for key, value in data.items():
                # Look for recipient addresses
                if key in ['recipient', 'to', 'beneficiary'] and isinstance(value, str) and value.startswith('osmo1'):
                    # Look for associated amounts
                    amount_keys = ['amount', 'total', 'value', 'sum']
                    for amount_key in amount_keys:
                        if amount_key in data:
                            amount_value = data[amount_key]
                            if isinstance(amount_value, (str, int)) and str(amount_value).isdigit():
                                amount = int(amount_value)
                                payment = base_payment.copy()
                                payment.update({
                                    'Recipient': value,
                                    'Amount (uosmo)': str(amount),
                                    'Amount (Raw)': amount,
                                    'Amount (OSMO)': amount / 1000000,  # Default to uosmo conversion
                                    'Denom': 'uosmo'  # Default denomination
                                })
                                payments.append(payment)
                                break
                
                # Recursively search nested structures
                self._extract_recursive_payments(value, payments, base_payment)
        
        elif isinstance(data, list):
            for item in data:
                self._extract_recursive_payments(item, payments, base_payment)
    
    def _load_pricing_data(self):
        """
        Load pricing data from the attached file
        """
        try:
            with open('attached_assets/combined_daily_prices_1756000184191.json', 'r') as f:
                pricing_list = json.load(f)
            
            # Convert list to dictionary for faster lookups: {token: {date: price}}
            self.pricing_lookup = {}
            for entry in pricing_list:
                token = (entry.get('token') or '').strip()
                # Normalize token key to uppercase for robust matching (e.g., 'scrt' -> 'SCRT')
                token_key = token.upper() if token else token
                date = entry.get('date')
                price = entry.get('price')

                if not token_key:
                    continue
                if token_key not in self.pricing_lookup:
                    self.pricing_lookup[token_key] = {}
                self.pricing_lookup[token_key][date] = price
                
        except (FileNotFoundError, json.JSONDecodeError) as e:
            # If pricing data file is not available, use empty dict
            self.pricing_lookup = {}
    
    def _extract_proposal_date(self, proposal: Dict[str, Any]) -> str:
        """
        Extract proposal date from proposal data
        """
        # Try different ways to get the date
        try:
            # Look for created_at timestamp
            if 'created_at' in proposal:
                timestamp = proposal['created_at']
                if isinstance(timestamp, str) and 'T' in timestamp:
                    return timestamp.split('T')[0]
            
            # Look for expiration and work backwards
            if 'expiration' in proposal:
                exp = proposal['expiration']
                if isinstance(exp, dict) and 'at_time' in exp:
                    timestamp = exp['at_time']
                    if isinstance(timestamp, str):
                        # Parse nanosecond timestamp
                        try:
                            # Convert nanoseconds to seconds
                            timestamp_seconds = int(timestamp) / 1000000000
                            date_obj = datetime.fromtimestamp(timestamp_seconds)
                            # Estimate proposal date as a few days before expiration
                            proposal_date = date_obj - timedelta(days=7)
                            return proposal_date.strftime('%Y-%m-%d')
                        except (ValueError, OverflowError):
                            pass
            
            # Default to current date if no date found
            return datetime.now().strftime('%Y-%m-%d')
            
        except Exception:
            return datetime.now().strftime('%Y-%m-%d')
    
    def _calculate_usd_value(self, row) -> float:
        """
        Calculate USD value for a transaction
        """
        try:
            adjusted_amount = row.get('Adjusted Amount', 0)
            token_symbol = row.get('Display Symbol', '')
            proposal_date = row.get('Proposal Date', '')
            
            if not adjusted_amount or not token_symbol or not proposal_date:
                return 0.0
            
            # Prepare candidate keys to try for pricing lookup. Pricing tokens are often uppercase tickers.
            candidates = []
            try:
                # Use Display Symbol directly, then a few normalized variants
                candidates.append(token_symbol)
                candidates.append(token_symbol.upper())
                candidates.append(token_symbol.lower())
                # If token symbol looks like an IBC denom, use the last segment
                if '/' in token_symbol:
                    last = token_symbol.split('/')[-1]
                    candidates.append(last)
                    candidates.append(last.upper())
                # Remove non-alphanumeric characters
                import re
                clean = re.sub(r'[^A-Za-z0-9]', '', token_symbol)
                if clean:
                    candidates.append(clean)
                    candidates.append(clean.upper())
            except Exception:
                candidates = [token_symbol, token_symbol.upper()]

            # Known alias map (common mismatches)
            alias_map = {
                'uosmo': 'OSMO',
                'osmo': 'OSMO',
                'scrt': 'SCRT'
            }

            # Try each candidate until a price mapping is found
            for key in candidates:
                if not key:
                    continue
                # Map aliases
                lookup_key = alias_map.get(key.lower(), key)
                lookup_key_up = lookup_key.upper() if isinstance(lookup_key, str) else lookup_key
                if lookup_key_up in self.pricing_lookup:
                    token_prices = self.pricing_lookup[lookup_key_up]
                    # Try exact date match first
                    if proposal_date in token_prices:
                        return adjusted_amount * token_prices[proposal_date]

                    # If exact date not found, find closest date
                    available_dates = sorted(token_prices.keys())
                    if available_dates:
                        target_date = datetime.strptime(proposal_date, '%Y-%m-%d')
                        closest_date = min(available_dates, 
                                         key=lambda x: abs((datetime.strptime(x, '%Y-%m-%d') - target_date).days))
                        return adjusted_amount * token_prices[closest_date]
            
            # If not found in local pricing data, try CoinGecko historical price as a fallback
            try:
                # Simple mapping from symbol to CoinGecko id for common tokens
                cg_map = {
                    'SCRT': 'secret',
                    'OSMO': 'osmosis',
                    'PAGE': 'page'
                }
                # Determine a symbol to try
                sym = token_symbol.upper()
                cg_id = cg_map.get(sym)
                if cg_id:
                    # Initialize cache if needed
                    if not hasattr(self, '_cg_cache'):
                        self._cg_cache = {}

                    cache_key = f"{cg_id}:{proposal_date}"
                    if cache_key in self._cg_cache:
                        price = self._cg_cache[cache_key]
                        return adjusted_amount * price if price is not None else 0.0

                    # Query CoinGecko for historical price: date format is DD-MM-YYYY
                    try:
                        from requests import get
                        date_parts = proposal_date.split('-')
                        if len(date_parts) == 3:
                            dd = f"{int(date_parts[2]):02d}-{int(date_parts[1]):02d}-{int(date_parts[0])}"
                            url = f"https://api.coingecko.com/api/v3/coins/{cg_id}/history?date={dd}"
                            resp = get(url, timeout=6)
                            if resp.status_code == 200:
                                j = resp.json()
                                price = j.get('market_data', {}).get('current_price', {}).get('usd')
                                # Cache the result (may be None)
                                self._cg_cache[cache_key] = price
                                return adjusted_amount * price if price is not None else 0.0
                    except Exception:
                        # network or parsing errors — fall through to zero
                        pass
            except Exception:
                pass

            return 0.0
            
        except Exception:
            return 0.0
