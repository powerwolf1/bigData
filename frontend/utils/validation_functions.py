import requests
from frontend.utils.config import API_URL


def parse_collection(endpoint):
    try:
        response = requests.get(f"{API_URL}/{endpoint}")
        response.raise_for_status()
        return response.json().get('message', 'Parsing successful')
    except requests.exceptions.RequestException as e:
        return f"Error  parsing data: {e}"

