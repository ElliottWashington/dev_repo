import pandas as pd
import yfinance as yf
from scipy.stats import norm
import zipfile
import mysql.connector
import glob
from datetime import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import re
import numpy as np

date = datetime.now().strftime('%Y%m%d')

mapping = {
    0: "Vertical",
    1: "Calendar",
    2: "Ratio",
    3: "Collar",
    4: "Combo",
    5: "Straddle",
    6: "Strangle",
    7: "Butterfly",
    8: "Condor",
    9: "IronCondor",
    10: "BuyWrite",
    11: "Diagonal",
    12: "Box",
    13: "BrokenButterfly",
    14: "IronButterfly",
    15: "DoubleDiagonal",
    16: "DoubleCalendar",
    20: "Conversion",
    23: "CoveredStrangle",
    24: "StraddleWithStock",
    25: "StraddleSwap",
    98: "Custom",
    99: "Unknown"
}

def process_spread_legs(spread_legs):
    def extract_strike_price(leg):
        try:
            # Use regex to find the ticker (up to the first digit), the date (the following 8 digits), 
            # the option type (C or P), and the rest
            pattern = re.compile(r"([A-Z]+)(\d{8})([CP])(.*)")

            matches = pattern.match(leg)
            if matches is None:
                print(f"Failed to process leg: {leg}")
                return None

            ticker, date_string, option_type, rest = matches.groups()

            # In the rest part, the strike price is the number part before the first non-digit character.
            strike_price = re.search(r"(\d*\.?\d*)", rest).group()

            return float(strike_price)

        except Exception as e:
            print(f"Failed to process leg: {leg}")
            print(f"Error: {e}")
            return None

    # Split the legs by '_'
    spread_legs = spread_legs.split("_")

    # Prepare lists for sortable and non-sortable legs
    sortable_legs = []
    non_sortable_legs = []

    for leg in spread_legs:
        strike_price = extract_strike_price(leg)
        if strike_price is None:
            non_sortable_legs.append(leg)
        else:
            sortable_legs.append((strike_price, leg))

    # Sort the sortable legs by strike price
    sortable_legs.sort()

    # Combine back the sorted legs with non-sortable legs, ignoring None values in strike prices
    return "_".join([leg[1] for leg in sortable_legs] + non_sortable_legs)

def extract_leg_data(leg):
    try:
        # Use regex to find the ticker (up to the first digit), the date (the following 8 digits), and the rest
        pattern = re.compile(r"([A-Z]+)(\d{8})([CP])(.*)")

        matches = pattern.match(leg)
        if matches is None:
            print(f"Failed to process leg: {leg}")
            return None

        ticker, date_string, option_type, rest = matches.groups()

        # Identify option type
        if option_type == 'C':
            option_type = "Call"
        else:
            option_type = "Put"

        # Find position type
        position_type = re.search(r"(B\d+|S\d+)", rest)
        if not position_type:
            print(f"Failed to identify position type in leg: {leg}")
            return None

        if 'B' in position_type.group():
            rest = rest.replace(position_type.group(), "")
            position_type = "Long"
        else:
            rest = rest.replace(position_type.group(), "")
            position_type = "Short"

        # Extract strike price
        strike_price = float(rest)

        # Extract expiry date
        expiry_date = datetime.strptime(date_string, "%Y%m%d")

        # Prepare and return extracted data
        leg_data = {
            "ticker": ticker,
            "expiry_date": expiry_date,
            "strike_price": strike_price,
            "option_type": option_type,
            "position_type": position_type
        }
        print(leg_data)
        return leg_data

    except Exception as e:
        print(f"Failed to process leg: {leg}")
        print(f"Error: {e}")
        return None

def calculate_delta(leg, undly_price, volatility, r=0.01):
    if leg is None:
        print("Leg is None, cannot calculate delta")
        return None
    # Assume leg is a dictionary with 'strike_price', 'expiry_date', and 'option_type'
    S = undly_price
    K = leg['strike_price']
    T = (leg['expiry_date'] - datetime.today()).days / 365
    σ = volatility
    d1 = (np.log(S / K) + (r + (σ**2) / 2) * T) / (σ * np.sqrt(T))

    if leg['option_type'] == 'C':
        return norm.cdf(d1)
    elif leg['option_type'] == 'P':
        return -norm.cdf(-d1)
    else:
        print(f"Unexpected option type {leg['option_type']} found in leg: {leg}")
        return None  # Return None if option type is not as expected
    
def calculate_option_price(leg, undly_price, volatility, r=0.01):
    if leg is None:
        print("Leg is None, cannot calculate option price")
        return None
    S = undly_price
    K = leg['strike_price']
    T = (leg['expiry_date'] - datetime.today()).days / 365
    σ = volatility
    d1 = (np.log(S / K) + (r + (σ**2) / 2) * T) / (σ * np.sqrt(T))
    d2 = d1 - σ * np.sqrt(T)

    if leg['option_type'] == 'C':
        return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif leg['option_type'] == 'P':
        return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    
def process_row_for_option_price(row):
    legs = row['formatted_symbol'].split('_')
    prices = []
    for leg_str in legs:
        leg = parse_leg(leg_str)
        if leg is not None:
            price = calculate_option_price(leg, row['LastPrice'], row['Volatility'])
            if price is not None:
                prices.append(price)
    return sum(prices) 

def process_row_for_delta(row):
    # Split the formatted_symbol string into its component leg strings
    legs = row['formatted_symbol'].split('_')

    deltas = []
    for leg_str in legs:
        # Parse leg_str to extract the components of the option contract
        leg = parse_leg(leg_str)
        if leg is not None:
            delta = calculate_delta(leg, row['LastPrice'], row['Volatility'])
            if delta is not None:
                deltas.append(delta)
    return sum(deltas) if deltas else None

def parse_leg(leg_str):
    pattern = re.compile(r'(\w+)(\d{8})[CP](\d+.\d+)[BS]\d+')
    match = pattern.match(leg_str)
    
    if match:
        ticker, expiry_date, strike_price = match.groups()

        # Convert expiry_date to datetime
        expiry_date = datetime.strptime(expiry_date, '%Y%m%d')

        # Convert strike_price to float
        strike_price = float(strike_price)

        # Determine the option_type based on 'C' or 'P' in the string
        option_type = 'C' if 'C' in leg_str else 'P'

        return {
            'ticker': ticker,
            'expiry_date': expiry_date,
            'strike_price': strike_price,
            'option_type': option_type
        }
    else:
        print(f"Error parsing leg: {leg_str}. Not matching expected pattern.")
        return None

def get_prev_day_volumes(df):
    symbols = df['underly'].unique().tolist()
    
    # Try to download data for all symbols at once
    try:
        data = yf.download(symbols, period='2d')
    except Exception as e:
        return {}
        
    # data is a DataFrame with MultiIndex columns. The top level of the index is the variable name ('Open', 'High', 'Low', etc.),
    # the second level is the symbol. We're interested in 'Volume' for all symbols.
    volume_data = data['Volume']
    
    # The volume of the previous day for each symbol is the second last data point
    volumes = volume_data.iloc[-2].to_dict()
    
    # Replace NaN values (missing data) with a default value
    for symbol in symbols:
        if np.isnan(volumes.get(symbol, np.nan)):
            volumes[symbol] = None  # Or any other default value
            
    return volumes

def calculate_edge(row):
    if (row['BBOBid'] - row['LastPrice']) > 0:
        return row['BBOBid'] - row['LastPrice']
    elif (row['LastPrice'] - row['BBOAsk']) > 0:
        return row['LastPrice'] - row['BBOAsk']
    else:
        return 0  # If neither condition is met, return 0

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

def get_data(date):
    mydb = mysql.connector.connect(
        user='RBadmin',
        password='$Calp123',
        host='10.5.1.32',
        database='rbandits2',
        ssl_disabled=True
    )
    query = f"""
        SELECT ts as Time, underly, sprdsym as Spread, price as LastPrice, sprdtype
        FROM trdsprd
        WHERE ts >= DATE_SUB('{date}', INTERVAL 3 DAY)
        and sprdtype in ('0', '5')
        ORDER BY ts DESC
    """
    df = pd.read_sql_query(query, mydb)
    mydb.close()
    return df

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

def get_historical_data(tickers, start_date, end_date):
    data = {}
    
    for ticker in tickers:
        stock = yf.Ticker(ticker)
        data[ticker] = stock.history(start=start_date, end=end_date)
        
    return data

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

def run():
    lower = 1
    upper = 50
    main_df = pd.read_csv(f'/home/elliott/Development/scripts/jupyter_notebooks/spreads-202308071018.csv')
    main_df = main_df[['Symbol', 'BBOAsk', 'BBOBid', 'BBOBidSize', 'BBOAskSize']]
    main_df = main_df[(main_df['BBOBid'] != 0) | (main_df['BBOAsk'] != 0)]
    main_df['formatted_symbol'] = main_df['Symbol'].apply(convert_to_new_format)
    db_df = get_data(date)
    #merged_df = main_df.join(db_df, how='left')
    merged_df = main_df.merge(db_df, how='left', left_on='formatted_symbol', right_on='Spread')
    merged_df.dropna(inplace=True)
    merged_df.drop(columns=['Symbol','Spread'], inplace=True)
    merged_df['sprdtype'] = merged_df['sprdtype'].map(mapping)
    merged_df['sprdtype'] = merged_df.apply(identify_option_type, axis=1)
    merged_df['Inverted'] = merged_df.apply(identify_inverted, axis=1)
    merged_df.sort_values(by='Time', ascending=False, inplace=True)
    merged_df.drop_duplicates(subset='formatted_symbol', keep='first', inplace=True)
    merged_df.to_csv("merged_df.csv")
    merged_df['undlyPriceAtTime'] = np.random.uniform(lower, upper, size=len(merged_df)).round(2)
    tickers = merged_df['underly'].unique().tolist()
    print(tickers)
    data = get_historical_data(tickers, "2023-08-01", "2023-08-07")
    all_data = []
    for ticker, df in data.items():
        df = df.reset_index()
        df['Ticker'] = ticker
    all_data.append(df[['Date', 'Ticker', 'Close']])
    print(all_data)
    historical_df = pd.concat(all_data)
    historical_df.to_csv("historical_df.csv")
    historical_df['Date'] = historical_df['Date'].dt.tz_localize(None)
    result_df = pd.merge(merged_df, historical_df, left_on=['Time', 'underly'], right_on=['Date', 'Ticker'])
    #result_df.drop(['Date', 'Ticker'], axis=1, inplace=True)
    result_df.to_csv("result_df.csv")
    #result_df['undlyPriceAt815'] = result_df['undlyPriceAtTime'] * (1 + np.random.uniform(-0.15, 0.15, size=len(merged_df)))
    #result_df['undlyPriceAt815'] = result_df['undlyPriceAt815'].round(2)
    #result_df['LastPriceDelta'] = result_df['undlyPriceAt815'] - merged_df['undlyPriceAtTime']
    #result_df['LastPriceDelta'] = result_df['LastPriceDelta'].round(2)
    #result_df['Volatility'] = np.random.uniform(0.10, 0.60, size=len(merged_df))
    selected_columns = ['formatted_symbol', 'LastPrice', 'Volatility']
    subset_df = result_df[selected_columns]
    result_df['Delta'] = subset_df.apply(process_row_for_delta, axis=1)
    result_df['BBOAsk'] = result_df['BBOAsk'].abs()
    result_df['BBOBid'] = result_df['BBOBid'].abs()
    result_df['LastPrice'] = result_df['LastPrice'].abs()
    result_df['Bid Edge'] = np.where(result_df['BBOBid'] != 0, result_df['BBOBid'] - result_df['LastPrice'], np.nan)
    result_df['Offer Edge'] = np.where(result_df['BBOAsk'] != 0, result_df['LastPrice'] - result_df['BBOAsk'], np.nan)
    verticals_df = result_df[result_df['sprdtype'].str.contains("Vertical")]
    straddle_df = result_df[result_df['sprdtype'].str.contains("Straddle")]
    straddle_df.to_csv("straddle.csv")
    verticals_df.to_csv("verticals.csv")

run()
