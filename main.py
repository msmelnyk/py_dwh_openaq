import requests, os, time, datetime
# from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv('BASE_URL')
TEST_LOCATION = os.getenv('TEST_LOCATION')

API_KEY = os.getenv('API_KEY')

def fetch_measurements():
    r = requests.get(BASE_URL + TEST_LOCATION, headers={"X-API-Key":API_KEY}, timeout=30)
    if r.status_code == 429:
        print("HTTP 429: rate limited")
    if r.status_code == 500:
        print("HTTP 500: " + str(r.headers))
    else:
        print(r.status_code)
        print(r.json())
    # r.raise_for_status()
    # return r.json()

if __name__ == '__main__':
    fetch_measurements()
