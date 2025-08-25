import requests

class DAOAPIClient:
	def __init__(self, base_url: str, network: str = 'osmosis-1'):
		self.base_url = base_url.rstrip('/')
		self.network = network

	def _get(self, path: str, params=None, timeout=15):
		url = f"{self.base_url}{path}"
		r = requests.get(url, params=params or {}, timeout=timeout)
		r.raise_for_status()
		return r.json()

	def get_dao_info(self, dao_address: str):
		try:
			return self._get(f"/daos/{dao_address}")
		except Exception:
			# Fallback or return a minimal structure
			return {'config': {'name': 'Main DAO'}, 'address': dao_address}

	def get_subdaos(self, dao_address: str):
		try:
			return self._get(f"/daos/{dao_address}/subdaos")
		except Exception:
			return []

	def get_proposals(self, dao_address: str):
		try:
			return self._get(f"/daos/{dao_address}/proposals")
		except Exception:
			return []

	def get_dao_members(self, dao_address: str):
		try:
			return self._get(f"/daos/{dao_address}/members")
		except Exception:
			return []