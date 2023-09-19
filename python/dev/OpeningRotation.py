import time
from concurrent.futures import ThreadPoolExecutor, thread
import concurrent.futures
import pandas as pd
from datetime import datetime, timedelta
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import re
import numpy as np
import psycopg2
from scipy.stats import norm
import scipy.stats as stats
from sqlalchemy import create_engine
import zipfile
from functools import wraps
from typing import Any

connection = psycopg2.connect(host="10.5.1.20", database="marketdata", user="elliott", password="scalp", port='5432')
connection2 = psycopg2.connect(host="10.7.8.59", database="theoretical", user="scalp", password="QAtr@de442", port='5433')
connection3 = psycopg2.connect(host="10.7.8.59", database="fixtransactions", user="scalp", password="QAtr@de442", port='5433')

m_root = 'RBadmin'
m_password = '$Calp123'
m_host = '10.5.1.32'
m_db = 'rbandits2'
uri = f"mysql+mysqldb://{m_root}:{m_password}@{m_host}/{m_db}"
mydb = create_engine(uri, connect_args={'ssl_mode': 'DISABLED'})
mapping = {0: "Vertical",5: "Straddle", 6: "Strangle"}

def timing_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        print(f"Elapsed time for {func.__name__}: {end_time - start_time} seconds")
        return result
    return wrapper

@timing_decorator
def load_and_preprocess_main_df(date):
    main_df = pd.read_csv(f'/home/elliott/Development/files/spreads-{date}0915.csv')
    main_df = main_df[['Symbol', 'BBOAsk', 'BBOBid', 'BBOBidSize', 'BBOAskSize']].copy()
    main_df = main_df[(main_df['BBOBid'] != 0) | (main_df['BBOAsk'] != 0)].copy()
    main_df['formatted_symbol'] = main_df['Symbol'].apply(convert_to_new_format)
    return main_df

def convert_to_new_format(option):
    if pd.isnull(option):
        return ''  # Return an empty string for NaN values

    parts = option.split('_')  # Split the spread into individual options
    new_parts = []
    for part in parts:
        # Identify the beginning of the date substring
        date_start = None
        for i in range(len(part) - 5):  # Subtract 5 to avoid running off the end of the string
            if part[i:i+2].isdigit() and part[i+2:i+4].isdigit() and part[i+4:i+6].isdigit():
                date_start = i
                break
        # If a date substring couldn't be found, treat the part as a non-option
        if date_start is None:
            new_parts.append(part)
            continue
        # Extract underlying, date, call/put, strike, and suffix
        underlying = part[:date_start]
        date = '20' + part[date_start:date_start+6]  # Convert YY to YYYY
        cp = part[date_start+6]
        strike_start = date_start + 7
        strike_end = strike_start
        for char in part[strike_start:]:
            if char.isdigit() or char == '.':
                strike_end += 1
            else:
                break
        strike = part[strike_start:strike_end]
        # Append decimal and trailing zeros if necessary
        if '.' not in strike:
            strike += '.00'
        elif len(strike.split('.')[1]) == 1:
            strike += '0'
        suffix = part[strike_end:]
        new_part = underlying + date + cp + strike + suffix
        new_parts.append(new_part)
    return '_'.join(new_parts)  # Join the options back into a spread

@timing_decorator
def merge_and_process(main_df, date):
    db_df = get_data_parallel([date])
    merged_df = main_df.merge(db_df, how='left', left_on='formatted_symbol', right_on='Spread').copy()
    merged_df.dropna(inplace=True)
    merged_df['sprdtype'] = merged_df['sprdtype'].map(mapping)
    merged_df['sprdtype'] = merged_df.apply(identify_option_type, axis=1)
    merged_df['Inverted'] = merged_df.apply(identify_inverted, axis=1)
    merged_df = merged_df.apply(process_put_verticals, axis=1)
    merged_df.sort_values(by='Time', ascending=False, inplace=True)
    merged_df.drop_duplicates(subset='formatted_symbol', keep='first', inplace=True)
    return merged_df

def process_put_verticals(row):
    if row['sprdtype'] == 'Put Vertical':
        # Swap BBOAsk and BBOBid
        row['BBOAsk'], row['BBOBid'] = row['BBOBid'], row['BBOAsk']
        
        # Swap BBOAskSize and BBOBidSize (assuming you have these columns)
        row['BBOAskSize'], row['BBOBidSize'] = row['BBOBidSize'], row['BBOAskSize']

        # Take absolute values of LastPrice, BBOBid, and BBOAsk
        row['LastPrice'] = abs(row['LastPrice'])
        row['BBOBid'] = abs(row['BBOBid'])
        row['BBOAsk'] = abs(row['BBOAsk'])
        
    return row

def identify_inverted(row):
    # Ignore the rows where either 'BBOAsk' or 'BBOBid' is zero
    if row['BBOAsk'] == 0 or row['BBOBid'] == 0:
        return ''
    # Check if 'BBOAsk' is less than 'BBOBid'
    elif row['BBOAsk'] < row['BBOBid']:
        return 'Inverted'
    else:
        return ''

def identify_option_type(row):
    # Check if the 'sprdtype' is 'Vertical'
    if row['sprdtype'] == 'Vertical':
        # Check for 'C' or 'P' surrounded by digits in 'formatted_symbol'
        matches = re.findall(r'\d(C|P)\d', row['formatted_symbol'])
        if matches:
            # Take the first match and check the middle character
            if 'C' in matches[0]:
                return 'Call Vertical'
            else:
                
                return 'Put Vertical'
    # If 'sprdtype' is not 'Vertical', return the original 'sprdtype'
    return row['sprdtype']

def get_data_parallel(date):
    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(get_data, date))
    return pd.concat(results, ignore_index=True)

@timing_decorator
def last_trading_date(date):
    query = f"""
            SELECT MAX(tradingdate)
            FROM tradingdates
            WHERE tradingdate < timestamp '{date}'
            """
    cursor = connection3.cursor()

    # Execute the query
    cursor.execute(query)
    result = cursor.fetchone()

    cursor.close()

    # Check if a result was found
    if result and result[0]:
        return result[0]
    else:
        return None

@timing_decorator
def get_data(last_trd_date):
    query = f"""
                SELECT ts as Time, underly, sprdsym as Spread, price as LastPrice, sprdtype
                FROM trdsprd
                WHERE ts >= {last_trd_date}
                AND sprdtype in ('0', '5', '6')
                AND underly NOT LIKE 'QQQ%%'
                AND underly NOT LIKE 'SPY%%'
                AND underly NOT LIKE 'IWM%%'
                ORDER BY ts DESC
            """
    df = pd.read_sql_query(query, mydb)
    return df

@timing_decorator
def get_batch_data2(df, current_date):
    # Extract relevant parameters from the dataframe
    symbols = df['underly'].unique()
    
    # Convert the current date string to a datetime.date object
    current_date_obj = datetime.strptime(current_date, '%Y%m%d').date()
    
    # Calculate the target times
    target_time_915 = datetime.combine(current_date_obj, pd.Timestamp("9:15:00").time())
    target_time_914 = datetime.combine(current_date_obj, pd.Timestamp("9:14:00").time())
    
    # Convert symbols list to a format suitable for SQL IN clause
    symbols_placeholder = ",".join(["%s"] * len(symbols))

    # Construct the query
    query = f"""
                SELECT symbol, tradets, tradevolume, tradelast
                FROM equity_trades_new
                WHERE 
                    symbol IN ({symbols_placeholder}) AND
                    (tradets BETWEEN %s AND %s)
            """
    
    cursor = connection.cursor()

    # Execute the query using parameters
    cursor.execute(query, (*symbols, target_time_914, target_time_915))
    results = cursor.fetchall()

    cursor.close()

    # Convert results to a dataframe for easier manipulation
    columns = ['Symbol', 'Timestamp', 'Volume', 'Last_Trade']
    batch_data_df = pd.DataFrame(results, columns=columns)

    return batch_data_df

@timing_decorator
def assign_equity_prices_at_915(original_df, batch_data_df, current_date):
    # Create a new column for price at 915
    original_df['price_at_915'] = None
    
    # Get a unique set of symbols
    unique_symbols = original_df['underly'].unique()

    # Process each unique symbol
    for symbol in unique_symbols:
        # Filter batch data for the current symbol
        batch_subset = batch_data_df[batch_data_df['Symbol'] == symbol]

        # If there are no matching records in batch_subset, skip this symbol
        if batch_subset.empty:
            continue
        
        # Convert the current_date string to a datetime.date object
        current_date_obj = datetime.strptime(current_date, '%Y%m%d').date()
        
        # Calculate the target time
        target_time = datetime.combine(current_date_obj, pd.Timestamp("9:15:00").time())
        
        time_diffs = batch_subset['Timestamp'].apply(lambda x: abs(datetime.combine(current_date_obj, x.time()) - target_time))
        closest_idx = time_diffs.idxmin()
        price_at_915 = batch_subset.loc[closest_idx, 'Last_Trade']
                
        # Assign the price to all rows with the matching symbol
        original_df.loc[original_df['underly'] == symbol, 'price_at_915'] = price_at_915
    return original_df

@timing_decorator
def get_batch_data1(df):
    # Extract relevant parameters from the dataframe
    min_time = df['Time'].min()
    max_time = df['Time'].max()
    symbols = df['underly'].unique()

    # Convert symbols list to a format suitable for SQL IN clause
    symbols_placeholder = ",".join(["%s"] * len(symbols))

    # Construct the query
    query = f"""
                SELECT symbol, tradets, tradevolume, tradelast
                FROM equity_trades_new
                WHERE 
                    symbol IN ({symbols_placeholder}) AND
                    tradets BETWEEN %s AND %s
            """

    cursor = connection.cursor()

    # Execute the query using parameters
    cursor.execute(query, (*symbols, min_time, max_time))
    results = cursor.fetchall()

    cursor.close()

    # Convert results to a dataframe for easier manipulation
    columns = ['Symbol', 'Timestamp', 'Volume', 'Last_Trade']
    batch_data_df = pd.DataFrame(results, columns=columns)

    return batch_data_df

@timing_decorator
def assign_closest_prices_parallel(original_df, batch_data_df):
    # Create a new DataFrame to store the results
    result_df = pd.DataFrame()

    # Create a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {executor.submit(process_symbol, original_df, batch_data_df, symbol): symbol for symbol in original_df['underly'].unique()}

        for future in concurrent.futures.as_completed(futures):
            symbol = futures[future]
            try:
                processed_subset = future.result()
                result_df = pd.concat([result_df, processed_subset])
            except Exception as e:
                print(f"An exception occurred while processing symbol {symbol}: {e}")

    return result_df

def process_symbol(original_df, batch_data_df, symbol):
    # Filter the original and batch DataFrames by the current symbol
    original_subset = original_df[original_df['underly'] == symbol].copy()
    batch_subset = batch_data_df[batch_data_df['Symbol'] == symbol].sort_values(by="Timestamp")

    if batch_subset.empty:
        return original_subset

    # Calculate the closest timestamp using vectorized operations
    closest_indices = original_subset['Time'].apply(
        lambda x: (batch_subset['Timestamp'] - x).abs().idxmin()
    )

    # Use the closest indices to lookup the closest trade price
    closest_trade_prices = batch_subset.loc[closest_indices, 'Last_Trade'].values

    # Assign the closest trade price to the original DataFrame
    original_subset['prc_eqt_at_time'] = closest_trade_prices

    return original_subset

@timing_decorator
def calculate_metrics_and_prices(merged_prices_df):
    merged_prices_df['equity_change_pct'] = ((merged_prices_df['prc_eqt_at_time'] - merged_prices_df['price_at_915']) / merged_prices_df['price_at_915']) * 100
    column_order = ['formatted_symbol', 'Symbol', 'Inverted', 'sprdtype', 'LastPrice', 'BBOBid', 'BBOBidSize', 'BBOAsk', 'BBOAskSize', 'Time', 'underly', 'prc_eqt_at_time', 'price_at_915']
    result_df = (merged_prices_df.loc[:, column_order]
                 .dropna(subset=['price_at_915', 'prc_eqt_at_time'])
                 .assign(PriceDelta=lambda df: df['price_at_915'].astype(float) - df['prc_eqt_at_time'],
                         BidEdge=lambda df: np.where(df['BBOBid'] != 0, df['BBOBid'] - df['LastPrice'], np.nan),
                         OfferEdge=lambda df: np.where(df['BBOAsk'] != 0, df['LastPrice'] - df['BBOAsk'], np.nan))
                 ).copy()
    result_df = round_to_two_decimals(result_df, columns=['BidEdge', 'OfferEdge', 'prc_eqt_at_time', 'price_at_915', 'PriceDelta', 'LastPrice', 'BBOBid', 'BBOAsk'])
    result_df['BidEdge'] = result_df['BidEdge'].astype(float)
    result_df['OfferEdge'] = result_df['OfferEdge'].astype(float)
    result_df = result_df.loc[(result_df['BidEdge'] < -0.20) | (result_df['OfferEdge'] < -0.20)]
    return result_df

def round_to_two_decimals(df, columns):
    """Format specified columns in a dataframe to two decimal places as strings."""
    for column in columns:
        df[column] = df[column].apply(lambda x: "{:.2f}".format(float(x)) if pd.notna(x) else None)
    return df

@timing_decorator
def generate_and_execute_sql_parallel(result_df):
    unique_underly_values = result_df['underly'].unique()
    with ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(generate_and_execute_sql, [result_df[result_df['underly'] == symbol] for symbol in unique_underly_values]))
    return {k: v for d in results for k, v in d.items()}

def generate_and_execute_sql(result_df):
    previous_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    new_df = result_df[['Symbol', 'underly']].copy()
    new_df['Symbol'] = new_df['Symbol'].str.split('_')
    exploded_df = new_df.explode('Symbol', ignore_index=True)
    unique_underly_values = exploded_df['underly'].unique()

    unique_underly_str = ','.join(f"'{symbol}%'" for symbol in unique_underly_values)
    unique_underly_str = f"ARRAY[{unique_underly_str}]"

    # Database connection
    connection = psycopg2.connect(host="10.7.8.59", database="theoretical", user="scalp", password="QAtr@de442", port='5433')
    cursor = connection.cursor()

    query = f"""
    SELECT DISTINCT ON (s.symbol) theo_rate, brents_vol, s.symbol, underlying
    FROM public.theo_rates r, public.theo_opt_sym_chain s
    WHERE r.theo_symbol_id = s.theo_symbol_id
    AND symbol LIKE ANY({unique_underly_str})
    ORDER BY s.symbol, r.create_time DESC;
    """
    
    cursor.execute(query)
    query_results = cursor.fetchall()
    cursor.close()
    connection.close()

    df = pd.DataFrame(query_results, columns=['theo_rate', 'volatility', 'symbol', 'underlying'])

    result_dict = {}

    for index, row in df.iterrows():
        symbol = row['symbol']
        theo_rate = row['theo_rate']
        volatility = row['volatility']
    
        result_dict[symbol] = {'theo_rate': theo_rate, 'volatility': volatility}

    return result_dict    

def lookup_rates_and_vols(row, rates_vol_dict):
    symbols = row['Symbol'].split('_')
    modified_symbols = [symbol[:-2] for symbol in symbols]
    
    if len(modified_symbols) != 2:
        print(f"Skipping row with symbols: {modified_symbols}")
        return row  # or set specific columns to NaN or some default value
    
    try:
        symbol1, symbol2 = modified_symbols
        row['rate1'] = rates_vol_dict[symbol1]['theo_rate']
        row['rate2'] = rates_vol_dict[symbol2]['theo_rate']
        row['vol1'] = rates_vol_dict[symbol1]['volatility']
        row['vol2'] = rates_vol_dict[symbol2]['volatility']
    except KeyError:
        print(f"Symbols {modified_symbols} not found in rates_vol_dict.")
        row['rate1'], row['rate2'], row['vol1'], row['vol2'] = [np.nan]*4
    
    return row

def black_scholes(S, X, t, r, q, sigma, option_type='call'):
    d1 = (np.log(S / X) + (r - q + sigma ** 2 / 2) * t) / (sigma * np.sqrt(t))
    d2 = d1 - sigma * np.sqrt(t)
    
    if option_type == 'call':
        option_price = S * np.exp(-q * t) * stats.norm.cdf(d1) - X * np.exp(-r * t) * stats.norm.cdf(d2)
    elif option_type == 'put':
        option_price = X * np.exp(-r * t) * stats.norm.cdf(-d2) - S * np.exp(-q * t) * stats.norm.cdf(-d1)
    else:
        raise ValueError("Invalid option_type. Use 'call' or 'put'")
        
    return option_price

def black_scholes_delta(S, X, t, r, q, sigma, option_type='call'):
    d1 = (np.log(S / X) + (r - q + sigma ** 2 / 2) * t) / (sigma * np.sqrt(t))
    
    if option_type == 'call':
        delta = np.exp(-q * t) * stats.norm.cdf(d1)
    elif option_type == 'put':
        delta = np.exp(-q * t) * (stats.norm.cdf(d1) - 1)
    else:
        raise ValueError("Invalid option_type. Use 'call' or 'put'")
    
    return delta

def calculate_black_scholes_and_delta(row):
    S = float(row['prc_eqt_at_time'])  # Convert to float
    X1 = float(row['Symbol'].split('_')[0].split('C')[-1].split('P')[-1][:-2])  # Convert to float
    X2 = float(row['Symbol'].split('_')[1].split('C')[-1].split('P')[-1][:-2])  # Convert to float
    t = 30 / 365  # Assuming 30 days to expiration (replace with actual value)
    r1 = row['rate1']
    r2 = row['rate2']
    q = 0  # Assuming no dividend yield
    sigma1 = row['vol1']
    sigma2 = row['vol2']
    
    bs_price1 = black_scholes(S, X1, t, r1, q, sigma1)
    bs_price2 = black_scholes(S, X2, t, r2, q, sigma2)
    
    delta1 = black_scholes_delta(S, X1, t, r1, q, sigma1)
    delta2 = black_scholes_delta(S, X2, t, r2, q, sigma2)
    
    row['BS_Price1'] = bs_price1
    row['BS_Price2'] = bs_price2
    row['Delta1'] = delta1
    row['Delta2'] = delta2
    row['Spread_Delta'] = delta1 - delta2
    
    return row

def set1(df):
    condition1 = (
        (df['PriceDelta'] > 0) & 
        (df['sprdtype'] == 'Call Vertical') & 
        (df['OfferEdge'] > 0)
    ) | (
        (df['PriceDelta'] > 0) & 
        (df['sprdtype'] == 'Put Vertical') & 
        (df['BidEdge'] > 0)
    )
    
    condition2 = (
        (df['PriceDelta'] < 0) & 
        (df['sprdtype'] == 'Call Vertical') & 
        (df['BidEdge'] > 0)
    ) | (
        (df['PriceDelta'] < 0) & 
        (df['sprdtype'] == 'Put Vertical') & 
        (df['OfferEdge'] > 0)
    )
    
    return df[condition1 | condition2]

def set2(df):
    condition = (
        df['equity_change_pct'].abs() > 1
    ) & ((df['Spread_Delta'].abs() > .25))
    return df[condition]

def set3(df):
    condition1 = (df['distance_change'] >= 0) & (df['BidEdge'] >= -1)
    condition2 = (df['distance_change'] <= 0) & (df['OfferEdge'] >= -1)
    return df[condition1 | condition2]

@timing_decorator
def filter_and_save(result_df, date):
    result_df.drop(columns=["Symbol"], inplace=True)
    result_df['PriceDelta'] = pd.to_numeric(result_df['PriceDelta'], errors='coerce')
    result_df['OfferEdge'] = pd.to_numeric(result_df['OfferEdge'], errors='coerce')
    result_df['BidEdge'] = pd.to_numeric(result_df['BidEdge'], errors='coerce')
    verticals_df = result_df[result_df['sprdtype'].str.contains("Vertical")].copy()
    verticals_df['prc_eqt_at_time'] = pd.to_numeric(verticals_df['prc_eqt_at_time'], errors='coerce')
    verticals_df['price_at_915'] = pd.to_numeric(verticals_df['price_at_915'], errors='coerce')
    verticals_df['equity_change_pct'] = ((verticals_df['price_at_915'] - verticals_df['prc_eqt_at_time']) / verticals_df['prc_eqt_at_time']) * 100
    verticals_df.drop(columns=['rate1', 'rate2', 'vol1', 'vol2', 'BS_Price1', 'BS_Price2'], inplace=True)
    verticals_df.to_csv(f"verticals_{date}.csv")
    verticles_set1 = set1(verticals_df)
    verticles_set1.to_csv(f"verticles_set_1_{date}.csv")
    verticles_set2 = set2(verticals_df)
    verticles_set2.to_csv(f"verticles_set_2_{date}.csv")
    verticles_set3 = verticals_df[(verticals_df['BidEdge'] > 0.50) | (verticals_df['OfferEdge'] > 0.50)]
    verticles_set3.to_csv(f"verticles_set_3_{date}.csv")
    straddle_df = result_df[result_df['sprdtype'].str.contains("Straddle")].copy()
    straddle_df.drop(columns=['rate1', 'rate2', 'vol1', 'vol2', 'BS_Price1', 'BS_Price2'], inplace=True)
    straddle_df.to_csv(f"straddles_{date}.csv")
    strangle_df = result_df[result_df['sprdtype'].str.contains("Strangle")].copy()
    strangle_df.to_csv(f"strangles_{date}.csv")
    strangle_df['AverageStrike'] = strangle_df['formatted_symbol'].apply(extract_strike)
    strangle_df['prc_eqt_at_time'] = pd.to_numeric(strangle_df['prc_eqt_at_time'], errors='coerce')
    strangle_df['price_at_915'] = pd.to_numeric(strangle_df['price_at_915'], errors='coerce')
    strangle_df['D1'] = (strangle_df['prc_eqt_at_time'] - strangle_df['AverageStrike']).abs()
    strangle_df['D2'] = (strangle_df['price_at_915'] - strangle_df['AverageStrike']).abs()
    strangle_df['distance_change'] = strangle_df['D1'] - strangle_df['D2']
    strangle_df.drop(columns=['rate1', 'rate2', 'vol1', 'vol2', 'BS_Price1', 'BS_Price2'], inplace=True)
    strangle_set1 = set3(strangle_df)
    strangle_set1.to_csv(f"strangles_set_1_{date}.csv")

def extract_strike(symbol):
    try:
        # Use regex to search for strike prices.
        # The pattern assumes that the strike price is always between a 'C' or 'P' and 'B' or 'S' followed by any digit.
        call_strike_search = re.search(r'C(\d+\.\d+)[BS]\d+', symbol)
        put_strike_search = re.search(r'P(\d+\.\d+)[BS]\d+', symbol)
        
        if call_strike_search and put_strike_search:
            call_strike = float(call_strike_search.group(1))
            put_strike = float(put_strike_search.group(1))
            return (call_strike + put_strike) / 2
        else:
            print(f"Could not find strike prices in {symbol}")
            return None

    except Exception as e:
        print(f"Error while extracting strike from {symbol}: {e}")
        return None

@timing_decorator
def zip_and_send(date):
    zip_filename = f"e.watchlist_{date}.zip"
    straddle_csv = f"straddles_{date}.csv"
    vertical_csv = f"verticals_{date}.csv"
    verticles_set1 = f"verticles_set_1_{date}.csv"
    verticles_set2 = f"verticles_set_2_{date}.csv"
    verticles_set3 = f"verticles_set_3_{date}.csv"
    strangles_csv = f"strangles_{date}.csv"
    strangle_set1 = f"strangles_set_1_{date}.csv"

    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(straddle_csv, arcname=os.path.basename(straddle_csv))
        zip_file.write(vertical_csv, arcname=os.path.basename(vertical_csv))
        zip_file.write(verticles_set1, arcname=os.path.basename(verticles_set1))
        zip_file.write(verticles_set2, arcname=os.path.basename(verticles_set2))
        zip_file.write(verticles_set3, arcname=os.path.basename(verticles_set3))
        zip_file.write(strangles_csv, arcname=os.path.basename(strangles_csv))
        zip_file.write(strangle_set1, arcname=os.path.basename(strangle_set1))

    #Send the zip archive via email
    send_email(['ewashington@scalptrade.com', 'sleland@scalptrade.com', 'aiacullo@scalptrade.com', 'jfeng@scalptrade.com', 'jthakkar@scalptrade.com ', 'jwood@scalptrade.com'], 
            'Open Rotation Watchlist - Test', 
            attachment_path=zip_filename)

def send_email(names, subject, attachment_path):
    fromaddr = "reports@scalptrade.com"
    toaddr = names

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = ", ".join(toaddr)
    msg['Subject'] = '{}'.format(subject)

    # Attach the CSV file
    attachment = open(attachment_path, 'rb')
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
    msg.attach(part)

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(fromaddr, "sc@lptrade")
    text = msg.as_string()
    s.sendmail(fromaddr, toaddr, text)

@timing_decorator
def main():
    date = datetime.now().strftime('%Y%m%d')

    formatted_date = datetime.now().strftime('%Y-%m-%d')

    last_trd_date = last_trading_date(formatted_date)

    if last_trd_date:
        last_trd_date = last_trd_date.strftime('%Y%m%d')

    main_df = load_and_preprocess_main_df(date)
    
    merged_df = merge_and_process(main_df, last_trd_date)

    prices915_df = get_batch_data2(merged_df, date)

    assign_equity_prices_at_915(merged_df, prices915_df, date)

    pricesattime_df = get_batch_data1(merged_df)

    merged_prices_df = assign_closest_prices_parallel(merged_df, pricesattime_df)
    
    result_df = calculate_metrics_and_prices(merged_prices_df)

    rates_vol_dict = generate_and_execute_sql_parallel(result_df)
    
    result_df = result_df.apply(lookup_rates_and_vols, axis=1, args=(rates_vol_dict,))

    result_df.dropna(subset=['rate1'], inplace=True)
    
    result_df = result_df.apply(calculate_black_scholes_and_delta, axis=1)
    
    filter_and_save(result_df, date)

    zip_and_send(date)

    #new change

if __name__ == "__main__":
    main()