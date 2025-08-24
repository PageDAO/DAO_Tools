import requests
import json
from typing import List, Dict, Any
import streamlit as st

class DAOAPIClient:
    """Client for interacting with the DAO DAO indexer API"""
    
    def __init__(self, base_url: str, network: str):
        self.base_url = base_url.rstrip('/')
        self.network = network
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'User-Agent': 'DAO-Accounting-Tool/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
    
    def get_proposals(self, contract_address: str, filter_status: str = "passed") -> List[Dict[Any, Any]]:
        """
        Fetch all proposals for a given contract address
        
        Args:
            contract_address: The contract address to query
            filter_status: Filter proposals by status (default: "passed")
            
        Returns:
            List of proposal dictionaries
        """
        url = f"{self.base_url}/{self.network}/contract/{contract_address}/daoCore/allProposals"
        
        params = {
            'filter': filter_status
        }
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'proposals' in data:
                return data['proposals']
            elif isinstance(data, dict) and 'data' in data:
                return data['data'] if isinstance(data['data'], list) else [data['data']]
            else:
                # If the response is a single proposal object, wrap it in a list
                return [data] if isinstance(data, dict) else []
                
        except requests.exceptions.RequestException as e:
            st.error(f"API request failed: {str(e)}")
            raise Exception(f"Failed to fetch proposals for {contract_address}: {str(e)}")
        except json.JSONDecodeError as e:
            st.error(f"Failed to parse JSON response: {str(e)}")
            raise Exception(f"Invalid JSON response from API: {str(e)}")
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
            raise Exception(f"Unexpected error while fetching proposals: {str(e)}")
    
    def get_proposal_details(self, contract_address: str, proposal_id: int) -> Dict[Any, Any]:
        """
        Fetch details for a specific proposal
        
        Args:
            contract_address: The contract address
            proposal_id: The proposal ID
            
        Returns:
            Proposal details dictionary
        """
        url = f"{self.base_url}/{self.network}/contract/{contract_address}/daoCore/proposal/{proposal_id}"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to fetch proposal {proposal_id}: {str(e)}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON response from API: {str(e)}")
    
    def get_subdaos(self, main_dao_address: str) -> List[Dict[Any, Any]]:
        """
        Fetch list of sub-DAOs from a main DAO
        
        Args:
            main_dao_address: The main DAO contract address
            
        Returns:
            List of sub-DAO dictionaries
        """
        url = f"{self.base_url}/{self.network}/contract/{main_dao_address}/daoCore/listSubDaos"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'subDaos' in data:
                return data['subDaos']
            elif isinstance(data, dict) and 'data' in data:
                return data['data'] if isinstance(data['data'], list) else [data['data']]
            else:
                # If the response is a single sub-DAO object, wrap it in a list
                return [data] if isinstance(data, dict) else []
                
        except requests.exceptions.RequestException as e:
            st.error(f"API request failed: {str(e)}")
            raise Exception(f"Failed to fetch sub-DAOs for {main_dao_address}: {str(e)}")
        except json.JSONDecodeError as e:
            st.error(f"Failed to parse JSON response: {str(e)}")
            raise Exception(f"Invalid JSON response from API: {str(e)}")
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
            raise Exception(f"Unexpected error while fetching sub-DAOs: {str(e)}")

    def get_dao_info(self, dao_address: str) -> Dict[Any, Any]:
        """
        Fetch DAO information including name using dumpState endpoint
        
        Args:
            dao_address: The DAO contract address
            
        Returns:
            DAO information dictionary
        """
        url = f"{self.base_url}/{self.network}/contract/{dao_address}/daoCore/dumpState"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            # Return empty dict if we can't fetch info
            return {}
        except json.JSONDecodeError as e:
            return {}
        except Exception as e:
            return {}

    def get_dao_members(self, dao_address: str) -> List[Dict[Any, Any]]:
        """
        Fetch DAO members list
        
        Args:
            dao_address: The DAO contract address
            
        Returns:
            List of member dictionaries
        """
        url = f"{self.base_url}/{self.network}/contract/{dao_address}/daoCore/listMembers"
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'members' in data:
                return data['members']
            elif isinstance(data, dict) and 'data' in data:
                return data['data'] if isinstance(data['data'], list) else [data['data']]
            else:
                return [data] if isinstance(data, dict) else []
                
        except requests.exceptions.RequestException as e:
            # Return empty list if we can't fetch members
            return []
        except json.JSONDecodeError as e:
            return []
        except Exception as e:
            return []

    def test_connection(self) -> bool:
        """
        Test if the API is accessible
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Try to access the API documentation endpoint
            url = f"{self.base_url}/{self.network}/docs"
            response = self.session.get(url, timeout=10)
            return response.status_code == 200
        except:
            return False
