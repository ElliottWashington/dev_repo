import requests
import csv
import time
import json
import pandas as pd
import psycopg2
from psycopg2 import sql
import csv

BASE_URL = "https://srm-api.sterlingtradingtech.com/pts-risk"
USERNAME = "AOSScalp"
PASSWORD = "Scalp1"
mapping = {
        "3OB05285": "12P52852",
        "3OB05287": "14P52874",
        "3OB05259": "15P52595",
        "3OB05283": "16P52836",
        "3OB05286": "17P52867",
        "3OB05294": "18P529422",
        "3OB05295": "19P529523",
        "3OB05255": "1P525501",
        "3OB05292": "20P529221",
        "3OB05303": "21P530324",
        "3OB05306": "22P530624",
        "3OB05305": "23P530525",
        "3OB05311": "24P531126",
        "3OB05314": "25P531426",
        "3OB05315": "26P531527",
        "3OB05322": "27P532248",
        "3OB05323": "28P532349",
        "3OB05324": "29P532451",
        "3OB05258": "2P525802",
        "3OB05326": "30P532652",
        "3OB05327": "31P532753",
        "3OB05345": "32P534563",
        "3OB05346": "33P534612",
        "3OB05347": "34P534728",
        "3OB05360": "35P536046",
        "3OB05361": "36P536158",
        "3OB05362": "37P536299",
        "3OB05363": "38P536381",
        "3OB05364": "39P536458",
        "3OB05284": "3P528403",
        "3OB05365": "40P536571",
        "3OB05366": "41P536622",
        "3OB05367": "42P536787",
        "3OB05256": "4P525604",
        "3OB05257": "5P525705",
        "3OB05288": "6P528806",
        "3OB05260": "7P526007",
        "3OB05289": "8P528918",
        "3OB05261": "9P526109",
        "3OB05254": "SIPM5254"
    }

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

def call_api(endpoint, access_token, json_body):
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    url = f"{BASE_URL}{endpoint}"
    
    response = requests.post(url, headers=headers, json=json_body)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API call failed. Status code: {response.status_code}")

def positions_main(access_token):
    positions_json_body = ["3OB05285", "3OB05287", "3OB05259", "3OB05283", "3OB05286", 
        "3OB05294", "3OB05295", "3OB05255", "3OB05292", "3OB05303", "3OB05306", 
        "3OB05305", "3OB05311", "3OB05314", "3OB05315", "3OB05322", "3OB05323", 
        "3OB05324", "3OB05258", "3OB05326", "3OB05327", "3OB05345", "3OB05346", 
        "3OB05347", "3OB05360", "3OB05361", "3OB05362", "3OB05363", "3OB05364", 
        "3OB05284", "3OB05365", "3OB05366", "3OB05367", "3OB05256", "3OB05257", 
        "3OB05288", "3OB05260", "3OB05289", "3OB05261", "3OB0525"]
    
    positions_data = call_api("/service/account-positions", access_token, positions_json_body)
    return positions_data

def account_pos_run():
    access_token = get_access_token()
    print("Access token obtained:", access_token)

    positions = positions_main(access_token)

    rows = []
    for item in positions:
        account = item["account"]
        accountName = item["accountName"]
        for position in item["positions"]:
            row = {
                "account": account,
                "accountName": accountName,
                "symbol": position["symbol"],
                "pos": position["pos"]
            }
            rows.append(row)

    # Create a DataFrame from the rows list
    df = pd.DataFrame(rows)
    df['date'] = datetime.now()
    return df 

def write_data_to_db(df):
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="10.5.1.251",
        database="Risk",
        user="scalp",
        password="QAtr@de442",
        port="5432"
    )

    cursor = conn.cursor()

    insert = sql.SQL('INSERT INTO position_data VALUES {}').format(
        sql.SQL(',').join(map(sql.Literal, [tuple(x) for x in df.values]))
    )

    cursor.execute(insert)
    conn.commit()
    conn.close()

def main():  
    positions = account_pos_run()
    return positions

if __name__ == '__main__':
    positions_data = main()
    write_data_to_db(positions_data)
    # house_data.to_csv("house_data.csv")
    print("done")