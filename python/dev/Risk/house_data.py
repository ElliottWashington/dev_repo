import requests
import csv
import time
import json
import pandas as pd

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

def house_main(access_token):
    true = True
    house_json_body = ["3OB05285", 
                       "3OB05287", 
                       "3OB05259", 
                       "3OB05283", 
                       "3OB05286", 
                       "3OB05294", 
                       "3OB05295", 
                       "3OB05255", 
                       "3OB05292", 
                       "3OB05303",
                       "3OB05306", 
                       "3OB05305",
                       "3OB05311",
                       "3OB05314",
                       "3OB05315",
                       "3OB05322",
                       "3OB05323", 
                       "3OB05324",
                       "3OB05258",
                       "3OB05326",
                       "3OB05327",
                       "3OB05345",
                       "3OB05346", 
                       "3OB05347", 
                       "3OB05360", 
                       "3OB05361", 
                       "3OB05362", 
                       "3OB05363", 
                       "3OB05364", 
                       "3OB05284", 
                       "3OB05365", 
                       "3OB05366", 
                       "3OB05367", 
                       "3OB05256", 
                       "3OB05257", 
                       "3OB05288", 
                       "3OB05260", 
                       "3OB05289", 
                       "3OB05261", 
                       "3OB0525"]


    house_data = call_api("/service/house-policy", access_token, house_json_body)
    return house_data

def get_house_data():
    access_token = get_access_token()
    print("Access token obtained:", access_token)
    
    # Suppose your json is stored in 'house_data' variable
    house_data = house_main(access_token)
    print(f"House data obtained: {house_data}")

    data = []

    # Loop through the JSON object
    for entry in house_data:
        # Extract the necessary values
        account = entry["account"]
        account_name = entry["accountName"]
        smv = entry["smv"]
        sod_equity = entry["sodEquity"]
        sod_house_excess = entry["sodHouseExcess"]
        tims = entry["tims"]
        tims_addon = entry["timsAddon"]
        equity = entry["equity"]
        
        # Append to the data list
        data.append([account, account_name, smv, sod_equity, sod_house_excess, tims, tims_addon, equity])

    # Define the column names
    columns = ["account", "accountName", "smv", "sodEquity", "sodHouseExcess", "tims", "timsAddon", "equity"]

    # Create the dataframe
    df = pd.DataFrame(data, columns=columns)
    
    df["account"] = df["account"].replace(mapping)
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

    insert = sql.SQL('INSERT INTO house_data VALUES {}').format(
        sql.SQL(',').join(map(sql.Literal, [tuple(x) for x in df.values]))
    )

    cursor.execute(insert)
    conn.commit()
    conn.close()

def main():  
    house = get_house_data()
    return house

if __name__ == '__main__':
    house_data = main()
    # house_data.to_csv("house_data.csv")
    print("done")
