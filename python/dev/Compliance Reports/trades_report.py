import psycopg2
import pandas as pd
import requests
from io import StringIO
import os
from datetime import datetime
import numpy as np

def get_activity(date):
       # Establish a connection to the PostgreSQL database
       conn = psycopg2.connect(
              host="10.7.8.59",
              database="fixtransactions",
              user="scalp",
              password="QAtr@de442",
              port="5433"
       )

       query = f"""
              SELECT tag77 as open_close, tag59 as TIF, tag1 as account, tag167 as order_type, tag32 as quantity, tag151 as leaves_qty, tag17 as exec_id, tag37 as order_id, tag11 as cl_ord_id, tag52 as date, tag55 as symbol, 
                     tag200 as maturityMY, tag201 as put_call, tag202 as strike_price, tag205 as maturityD, tag541 as full_exp_date, tag54 as side,
                     tag39 as status, tag31 as price, tag49, COALESCE(tag9730, tag9882) as liquidity_code, tag30 as exchange, tag442 as spread_or_single,
                     tag40 as orderType
              FROM fixmsg
              WHERE tag52 >= TIMESTAMP '{date}'
              AND tag52 <= TIMESTAMP '{date}' + INTERVAL '1 DAY'
              AND (tag39 = '1' or tag39 = '2')
              AND tag1 NOT IN ('5646353', '3618282', '3618588', '3616407', '5647492')
              AND tag1 NOT LIKE 'AOS%'
              AND tag35 = '8'
              AND tag20 != '1'
              AND tag20 != '2'
              """

       # Open a cursor
       cursor = conn.cursor()

       # Execute the query
       cursor.execute(query)

       # Fetch all the results
       results = cursor.fetchall()

       # Close the cursor
       cursor.close()

       # Convert the results into a DataFrame
       df = pd.DataFrame(results, columns=['open_close', 'TIF', 'account', 'order_type', 'quantity', 'leaves_qty','exec_id', 'order_id', 'cl_ord_id', 'date', 'symbol', 'maturityMY', 'put_call', 'strike_price', 'maturityD', 'full_exp_date', 'side', 'status', 'price', 'tag49','liquidity_code', 'exchange', 'spread_or_single', 'orderType'])
       
       # Assuming df is your DataFrame
       df['put_call'] = df['put_call'].replace({0: 'put', 1: 'call'})
       df['side'] = df['side'].replace({"1": 'Buy', "2": 'Sell', "3": 'Buy minus', "4": 'Sell plus', "5": 'Sell short'})
       df['status'] = df['status'].replace({"1": 'Partially filled', "2": 'Filled'})
       df['spread_or_single'] = df['spread_or_single'].fillna(0).astype(int).replace({1: "single", 2: "spread", 3: "spread", 0: "single"})
       df['orderType'] = df['orderType'].astype(int).replace({1: "Market", 2: "Limit", 3: "Stop",4: "Stop Limit", 5: "Market on Close"})
       df['TIF'] = df['TIF'].astype(int).replace({0: "Day", 1: "GTC", 2: "OPG", 3: "IOC",4: "FOK", 5: "GTX", 6: "Good Till Date", 7: "At the Close"})
       df['open_close'] = df['open_close'].replace({"O":"Open", "C" : "Close", "R" : "Rolled", "F" : "FIFO"})
       # Create the "option_equity" column based on the "strike_price" values
       df['option_equity'] = df['strike_price'].apply(lambda x: 'equity' if pd.isnull(x) else 'option')
       df['Maturity'] = df.apply(lambda row: calculate_maturity(row), axis=1)

       return df 

def calculate_maturity(row):
    try:
        if row['full_exp_date'] is not None:
            expiration_date = row['full_exp_date']
        else:
            expiration_date = datetime.strptime(f"{int(row['maturityMY'])}{int(row['maturityD'])}", '%Y%m%d').strftime('%Y%m%d')
        return expiration_date
    except ValueError:
        return None

def change_url(date: str):
    # Convert the date string to a datetime object
    date_obj = datetime.strptime(date, "%Y-%m-%d")

    # Extract the year, month and day
    year = date_obj.year
    month = f"{date_obj.month:02d}"  # Pad with leading zero if necessary
    day = f"{date_obj.day:02d}"  # Pad with leading zero if necessary

    # Format the URL with the extracted year, month and day
    url = f"https://www.cboe.com/us/options/market_statistics/penny_tick_type/{year}/{month}/cone_options_rpt_penny_tick_type_{year}{month}{day}.csv-dl?mkt=cone"
    return url

def get_penny_list(date):
    # Use the function
    url = change_url(date)

    response = requests.get(url)
    assert response.status_code == 200, 'Failed to download data'

    data = StringIO(response.text)
    df = pd.read_csv(data)

    # Save the dataframe only if the file does not exist
    filename = f"pennies_pilot_{date}.csv"
    if not os.path.isfile(filename):
        df.to_csv(filename)
    return df

def get_penny_dict(date):
    penny_df = get_penny_list(date)
    penny_df = penny_df.rename(columns={'OSI Root':'Symbol'})
    penny_df = penny_df.drop(columns={'Underlying Description', 'Tick Type'})
    penny_df = penny_df[~penny_df['Symbol'].str.startswith(tuple('0123456789'))]
    
    penny_df = penny_df['Symbol']
    penny_dict = penny_df.to_dict()
    return penny_dict

def activity_run(date):
    activity_df = get_activity(date)
    penny_dict = get_penny_dict(date)

    # Assuming dict is your dictionary and df is your DataFrame
    activity_df['penny'] = activity_df['symbol'].isin(penny_dict.values())
    activity_df['penny'] = activity_df['penny'].replace({True: 'penny', False: 'non-penny'})
    return activity_df

def get_rates():   
    # Load rates
    rates = pd.read_csv("/home/elliott/Development/files/Rates_for_testing.csv")
    rates = rates.applymap(lambda s:s.lower() if type(s) == str else s)
    rates.columns = rates.columns.str.lower().str.replace(' ', '_')

    # Remove trailing spaces from object columns
    rates = rates.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    # Rename columns
    rates = rates.rename(columns={
        'single' : 'spread_or_single',
        'liquidity': 'liquidity_code',
        'specific_symbol': 'symbol',
        'flat_rate': 'fee',
    })
    # Convert 'fee' in rates to numeric
    rates['fee'] = rates['fee'].replace({'\$': '', ',': '', '\(': '-', '\)': ''}, regex=True).astype(float)
    return rates

def df_to_nested_dict(df):
    if len(df.columns) == 1:
        if df.values.size == 1: return df.values[0][0]
        return df.values.squeeze()
    grouped = df.groupby(df.columns[0])
    d = {k: df_to_nested_dict(g.iloc[:,1:]) for k,g in grouped}
    return d

def get_fee(row, rates_dict, default_fee):
    # convert row['exchange'] to lowercase
    exchange_code = row['exchange'].lower()
    
    if exchange_code not in rates_dict:
        return default_fee

    try:
        fee = rates_dict[exchange_code][row['penny']][row['spread_or_single']][row['liquidity_code']][row['symbol']]
    except KeyError:
        try:
            fee = rates_dict[exchange_code][row['penny']][row['spread_or_single']][row['liquidity_code']]['0']
        except KeyError:
            try:
                fee = rates_dict[exchange_code][row['penny']][row['spread_or_single']]['0']['0']
            except KeyError:
                try:
                    fee = rates_dict[exchange_code][row['penny']]['0']['0']['0']
                except KeyError:
                    try:
                        fee = rates_dict[exchange_code]['0']['0']['0']['0']
                    except KeyError:
                        fee = default_fee
    return fee

def run(date):
    default_fee = "No Fee"
    activity_df = activity_run(date)

    # Specify columns to convert to lowercase
    columns_to_lower = ["exchange", "penny", "spread_or_single", "liquidity_code", "symbol"]

    # Lowercase specific columns for activity_df
    for column in columns_to_lower:
        activity_df[column] = activity_df[column].str.lower().astype(str)
    
    rates = get_rates()

    # Lowercase specific columns for rates
    for column in columns_to_lower:
        rates[column] = rates[column].str.lower().astype(str)
    
    rates_dict = df_to_nested_dict(rates)
    activity_df['fee'] = activity_df.apply(lambda row: get_fee(row, rates_dict, default_fee), axis=1)
    full_report = activity_df
    return full_report, rates, activity_df

date = '2023-05-24'
full_report, rates, activity_df = run(date)
new_columns = {
    "date" : "Date & Time",
    "account" : "Account Num",
    "symbol" : "Under",
    "Maturity" : "Expiration",
    "strike_price" : "Strike",
    "put_call" : "Call/Put",
    "exchange" : "Exchange",
    "tag49" : "Broker",
    "side" : "Side",
    "status" : "Status",
    "price" : "Price",
    "quantity" : "Quantity",
    "TIF" : "TIF",
    "orderType" : "Order Type",
    "open_close" : "Open/Close",
    "liquidity_code" : "Liquidity",
    "exec_id" : "ExecID",
    "cl_ord_id" : "OrderID",
    "spread_or_single" : "Spread/Single",
    "option_equity" : "Option/Equity",
    "penny" : "Penny/Non-Penny",
    "fee" : "Fee"
}
full_report.rename(columns=new_columns, inplace = True)
full_report = full_report[["Date & Time", "Account Num", "Under", "Expiration", "Strike", "Call/Put", "Exchange", "Broker", "Side", "Status", "Price", "Quantity", "TIF", "Order Type", "Open/Close", "Liquidity", "ExecID", "OrderID", "Spread/Single", "Option/Equity", "Penny/Non-Penny", "Fee"]]
full_report.to_csv(f"/home/elliott/Development/files/Daily_Scalp_Activity_{date}.csv")
full_report 