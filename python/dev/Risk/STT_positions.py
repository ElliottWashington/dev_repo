import requests
import json

BASE_URL = "https://srm-api.sterlingtradingtech.com/pts-risk"  # For UAT, replace with the production URL when ready
USERNAME = "AOSScalp"
PASSWORD = "Scalp1"

def get_access_token():
    url = f"{BASE_URL}/oauth/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": USERNAME,
        "client_secret": PASSWORD
    }
    response = requests.post(url, data=payload)

    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Failed to obtain access token. Status code: {response.status_code}")

def call_api(endpoint, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    url = f"{BASE_URL}{endpoint}"
    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API call failed. Status code: {response.status_code}")

def main():
    access_token = get_access_token()
    print("Access token obtained:", access_token)

    # Replace 'your_endpoint' with the desired endpoint from the STT API documentation
    endpoint_data = call_api("/service/account-positions", access_token)
    print("API call result:", endpoint_data)
    return endpoint_data


positions = main()