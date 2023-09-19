import pandas as pd
from datetime import datetime
import psycopg2
import numpy as np
import scipy.stats as stats

def generate_and_execute_sql(result_df):
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
        SELECT theo_rate, brents_vol, s.symbol, underlying
        FROM public.theo_rates r, public.theo_opt_sym_chain s
        WHERE r.theo_symbol_id = s.theo_symbol_id
        AND symbol LIKE ANY({unique_underly_str})
        AND r.create_time > timestamp '2023-08-28 16:00:00'
        ORDER BY strike, r.create_time DESC;
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
    #print(f"Original symbols: {symbols}")  # Debug print
    try:
        symbol1, symbol2 = [symbol[:-2] for symbol in symbols]  # Removing last two characters from each symbol
        #print(f"Modified symbols: {symbol1}, {symbol2}")  # Debug print
        row['rate1'] = rates_vol_dict[symbol1]['theo_rate']
        row['rate2'] = rates_vol_dict[symbol2]['theo_rate']
        row['vol1'] = rates_vol_dict[symbol1]['volatility']
        row['vol2'] = rates_vol_dict[symbol2]['volatility']
    except KeyError:
        #print(f"Symbols {symbols} not found in rates_vol_dict.")
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
    S = row['prc_eqt_at_time']
    X1 = float(row['Symbol'].split('_')[0].split('C')[-1].split('P')[-1][:-2])
    X2 = float(row['Symbol'].split('_')[1].split('C')[-1].split('P')[-1][:-2])
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

if __name__ == "__main__":
    main_df = pd.read_csv(f'/home/elliott/Development/scripts/jupyter_notebooks/result_df.csv', index_col=0)
    vol_rates_dict = generate_and_execute_sql(main_df)
    main_df = main_df.apply(lookup_rates_and_vols, axis=1, args=(vol_rates_dict,))
    main_df.dropna(subset=['rate1'], inplace=True)
    main_df = main_df.apply(calculate_black_scholes_and_delta, axis=1)
    main_df.to_csv("main_df.csv")