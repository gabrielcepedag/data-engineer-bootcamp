import requests
from datetime import date
from loguru import logger

class SBApiClient:
    def __init__(self, api_key: str):
        self.base_url = "https://apis.sb.gob.do/estadisticas/v2"
        if api_key is None:
            raise Exception("The API key cannot be set to none")
        self.api_key = api_key
    
    def get_earnings_by_location(
            self, 
            entity_type: str, 
            start_date: date, 
            page_number: int,
            total_records: int = 1500,
        ) -> dict:
        '''
        Gets earnings for all entities of the given entity type for each province in the DR.

        Args:
            entity_type: the initials for the type of entity that one wishes to query (
                Examples:
                    BM = Banco Múltiple/Commercial Banks
                    BAyC = Banco de Ahorro y Crédito/Savings Banks
                )
            start_date: the starting date from which the API will be queried.
            total_records: the amount of records that the API will return in its main data array.

        Returns:
            A dictionary with earnings for an entity in a specific province, in a given month and year.

        Raises:
            Exception if response code is not 200.
        '''
        params = {
            "periodoInicial": f"{start_date.year}-{start_date.month:02d}", 
            "tipoEntidad": entity_type, 
            "paginas": page_number,
            "registros": total_records
        }
        headers={
            "Cache-Control": "no-cache",
            "Ocp-Apim-Subscription-Key": self.api_key,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }
        response = requests.get(
            f"{self.base_url}/captaciones/localidad", 
            params=params,
            headers=headers)
        if response.status_code == 200:
            return response.json()
        logger.error(f"Failed to extract data from SB API. Status Code: {response.status_code}. Response: {response.text}")
        return []