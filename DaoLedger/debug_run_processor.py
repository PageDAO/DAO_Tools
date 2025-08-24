import json
from utils.data_processor import DataProcessor
from app import load_token_data

# Load sample proposals file
with open('attached_assets/research and development - all passed proposals_1755980973963.json', 'r') as f:
    proposals_list = json.load(f)

# Simulate the API client structure: one sub-unit with these proposals
proposal_data = {
    'Test SubDAO': {
        'address': 'osmo1a40j922z0kwqhw2nn0nx66ycyk88vyzcs73fyjrd092cjgyvyjksrd8dp7',
        'proposals': proposals_list
    }
}

# Load token data the same way the app does so denom -> symbol resolution works
token_data = {}
try:
    token_data = load_token_data()
except Exception:
    token_data = {}

processor = DataProcessor(core_team_addresses=[], token_data=token_data)
processed = processor.process_all_proposals(proposal_data)

print('Processed type:', type(processed))
try:
    print('Rows:', len(processed))
    print('Columns:', list(processed.columns))
    print('First 5 rows:\n', processed.head(5).to_dict(orient='records'))
except Exception as e:
    print('Error printing dataframe:', e)

