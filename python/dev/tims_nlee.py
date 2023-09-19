import pandas as pd
import psycopg2
from datetime import datetime

def get_date():
    # Establish a connection to the MySQL database
    conn = psycopg2.connect(
        host="10.5.1.251",
        user="scalp_ro",
        password="scalp",
        database="Risk"
    )

    query1 = f"""
            SELECT timestamp
            FROM greek_data 
            order by timestamp desc
            limit 1
            """
    
    cursor = conn.cursor()
    cursor.execute(query1)
    date = cursor.fetchone()
    return date

# Function to execute the query and return the results as a DataFrame
def execute_query(date):
    # Establish a connection to the MySQL database
    conn = psycopg2.connect(
        host="10.5.1.251",
        user="scalp_ro",
        password="scalp",
        database="Risk"
    )


    # Define the SQL query with a placeholder for the date
    query2 = f"""
            SELECT timestamp, account, symbol, pos, optionprice, underlyingprice, volatility
            FROM greek_data 
            WHERE timestamp >= timestamp '{date}' 
            AND timestamp < timestamp '{date}' + INTERVAL '1 DAY'
            --AND extract(hour from timestamp ) 
            --AND extract(minute from timestamp )
            order by account, symbol
            """
    
    # Execute the query and fetch the results
    cursor = conn.cursor()
    cursor.execute(query2)
    results = cursor.fetchall()

    columns=["timestamp", "account", "symbol", "pos", "optionprice", "underlyingprice", "volatility"]

    # Create an empty DataFrame
    df = pd.DataFrame(results, columns=columns)

    # Close the cursor and the database connection
    cursor.close()
    conn.close()

    # Return the DataFrame
    return df

# mapping accounts and parsing options symbol
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
# Lambda function - tweaked to fit sterling's option symbol format
def extract_data(row):
    if row.startswith("."):
        symbol = ""
        expiration = ""
        call_put = ""
        Strike = ""

        # Extract symbol
        for char in row[1:]:
            if not char.isspace():
                symbol += char
            else:
                break
        
        # Extract expiration
        expiration = row[len(symbol)+2:len(symbol)+8]

        # Extract call/put
        call_put = row[len(symbol)+8]
    
        # Extract strike price
        Strike = row[len(symbol)+9:]
        Strike = float(Strike)/1000

        # Validate data
        if len(symbol) == 0 or len(expiration) != 6 or call_put not in ["C", "P"] or not isinstance(Strike, float):
            return pd.Series([None, None, None, None],
                            index=["Symbol", "Expiration", "Call/Put", "Strike"])
        else:
            return pd.Series([symbol, expiration, call_put, Strike],
                            index=["Symbol", "Expiration", "Call/Put", "Strike"])
    else:
        return pd.Series([None, None, None, None],
                        index=["Symbol", "Expiration", "Call/Put", "Strike"])
    
# OCC min lambda function
def occ_min(symbol, position, optionprice, underlying, mincharge):
    if not symbol.startswith("."):
        mktValue = position * underlying
        return pd.Series([mktValue, 0], index= ["Market Value", 'OCC Min'])
    
    if position < 0:
        return pd.Series([(position * optionprice * 100), position*float(mincharge)], index = ["Market Value", "OCC Min"])
    if position > 0:
        mktValue = position * optionprice * 100
        if mktValue < position*float(mincharge):
            return pd.Series([mktValue, -mktValue], index = ["Market Value", "OCC Min"])
        else:
            return pd.Series([mktValue, -position*float(mincharge)], index = ["Market Value", "OCC Min"])
#
# Valuation of European call options
# in Black-Scholes-Merton model
#
# (c) Dr. Yves J. Hilpisch
# Python for Finance, 2nd ed.
#

def bsm_call_value(S0, K, T, r, sigma):
    ''' Valuation of European call option in BSM model.
    Analytical formula.

    Parameters
    ==========
    S0: float
        initial stock/index level
    K: float
        strike price
    T: float
        maturity date (in year fractions)
    r: float
        constant risk-free short rate
    sigma: float
        volatility factor in diffusion term

    Returns
    =======
    value: float
        present value of the European call option
    '''
    from math import log, sqrt, exp
    from scipy import stats

    S0 = float(S0)
    d1 = (log(S0 / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = (log(S0 / K) + (r - 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    # stats.norm.cdf --> cumulative distribution function
    #                    for normal distribution
    value = (S0 * stats.norm.cdf(d1, 0.0, 1.0) -
             K * exp(-r * T) * stats.norm.cdf(d2, 0.0, 1.0))
    return value
def bsm_put_value(S0, K, T, r, sigma):
    ''' Valuation of European put option in BSM model.
    Analytical formula.

    Parameters
    ==========
    S0: float
        initial stock/index level
    K: float
        strike price
    T: float
        maturity date (in year fractions)
    r: float
        constant risk-free short rate
    sigma: float
        volatility factor in diffusion term

    Returns
    =======
    value: float
        present value of the European put option
    '''
    from math import log, sqrt, exp
    from scipy import stats

    S0 = float(S0)
    d1 = (log(S0 / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    d2 = (log(S0 / K) + (r - 0.5 * sigma ** 2) * T) / (sigma * sqrt(T))
    # stats.norm.cdf --> cumulative distribution function
    #                    for normal distribution
    value = (K * exp(-r * T) * stats.norm.cdf(-d2, 0.0, 1.0)) - S0 * stats.norm.cdf(-d1, 0.0, 1.0)
    return value

# Stress Test lambda function
import py_vollib
from py_vollib.black_scholes.implied_volatility import implied_volatility as iv

def stress_test(position, optionprice, underlying, imp_v, adjFactor, exp, option, strike, cpmUp, cpmDown):
    if position != 0:
        
        # for equities
        if option not in ["C", "P"]:
            scenarios = []
            mktValue = position * underlying
            step = ((float(cpmUp) + float(cpmDown))/1000)* float(adjFactor)
            currentInterval = ((-float(cpmDown)/100))* float(adjFactor)
            for i in range(11):
                if i == 5:
                    currentInterval = currentInterval + step
                else:
                    PnL = (currentInterval * mktValue)
                    scenarios.append(PnL)
                    currentInterval = currentInterval + step
            return pd.Series([scenarios[0], scenarios[1], scenarios[2], scenarios[3], 
                              scenarios[4], scenarios[5], scenarios[6], scenarios[7], 
                              scenarios[8], scenarios[9]],
                                    index=["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"])
        # for options
        else:
            # sets value for close to expiration (1/252)
            if exp == 0:
                exp = .004
            
            if optionprice == 0:
                optionprice = 0.00001
                
            # assuming interest
            interest = .0525

            scenarios = []
            mktValue = position * optionprice * 100
            step = ((float(cpmUp) + float(cpmDown))/1000)* float(adjFactor)
            currentInterval = ((-float(cpmDown)/100))* float(adjFactor)

            # calculating implied volatility - longer runtime. Data maybe in postgres?
            # imp_v = .20 # dummy value - need for variable scope later on
            #if exp > 0:
                # this library is fastest but has an error with deep in the money that passes an exception
                #try:
                 #   imp_v = iv(optionprice, underlying, strike, exp, interest, option.lower())
                #except Exception:
                 #   pass
        
            
            for i in range(11):
                if i == 5:
                    currentInterval = currentInterval + step
                else:
                    if option == "C":
                        o = bsm_call_value(underlying + underlying*currentInterval, strike, exp, interest, imp_v)
                    elif option == "P":
                        o = bsm_put_value(underlying + underlying*currentInterval, strike, exp, interest, imp_v)
                    PnL = (o-optionprice)* position * 100
                    scenarios.append(PnL)
                    currentInterval = currentInterval + step
            return pd.Series([scenarios[0], scenarios[1], scenarios[2], scenarios[3], 
                              scenarios[4], scenarios[5], scenarios[6], scenarios[7], 
                              scenarios[8], scenarios[9]],
                                    index=["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"])
        
        return pd.Series([None, None, None, None, None, None, None, None, None, None],
                                    index=["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"])
    
# offsetter function
def offset(scen1, scen2, scen3, scen4, scen5, scen6, scen7, scen8, scen9, scen10, offset):
    scenList = [scen1, scen2, scen3, scen4, scen5, scen6, scen7, scen8, scen9, scen10]
    if float(offset) > 0:
        for i in range(len(scenList)):
            if scenList[i] > 0:
                scenList[i] = scenList[i] * float(offset)/100
        return pd.Series([scenList[0], scenList[1], scenList[2], scenList[3], 
                      scenList[4], scenList[5], scenList[6], scenList[7], 
                      scenList[8], scenList[9]],
                            index=["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"])
    else:
        return pd.Series([scenList[0], scenList[1], scenList[2], scenList[3], 
                              scenList[4], scenList[5], scenList[6], scenList[7], 
                              scenList[8], scenList[9]],
                                    index=["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"])
    
def main():

    #Execute query and get date
    date = get_date()[0]
    raw_portfolio = execute_query(date)

    # Retrieve included products report from OCC
    url = 'https://marketdata.theocc.com/rbh-documentation?fileName=RBHCPMIncludedProductsReport&format=csv'
    raw_products = pd.read_csv(url)
    raw_products = raw_products[:-1]
    prod_df = raw_products.copy(deep=True)
    portfolio_df = raw_portfolio.copy(deep=True)

    # adding min charge column since min charge will always default to CPM Min Charge if eligible
    CPMMinCharges = prod_df['CPM Minimum Charge Per Contract'].tolist()
    CPMEligibility = prod_df['Product CPM Eligible'].tolist()
    RBHMinCharges = prod_df['RBH Minimum Charge Per Contract'].tolist()
    mincharge = []

    for i in range(0, len(CPMMinCharges)):
        if CPMEligibility[i] == 'Yes':
            mincharge.append(float(CPMMinCharges[i]))
        else:
            mincharge.append(float(RBHMinCharges[i]))

    prod_df['Minimum Charge'] = mincharge

    portfolio_df['account'] = portfolio_df['account'].map(mapping)
    portfolio_df[["Symbol", "Expiration", "Call/Put", "Strike"]] = portfolio_df["symbol"].apply(lambda x: extract_data(x))
    portfolio_df['Expiration'] = pd.to_datetime(portfolio_df['Expiration'], format='%y%m%d')

    # filter for only active and marginable positions
    activePortfolio_df = portfolio_df[(portfolio_df['pos'] != 0) & (portfolio_df['underlyingprice'] > 3)]

    activePortfolio_df = activePortfolio_df.dropna(how='all')
    activePortfolio_df.reset_index(drop=True, inplace=True)

    productType = prod_df['Product Type'].tolist()
    occSymbol = prod_df['Symbol'].tolist()
    symbol = activePortfolio_df['symbol'].tolist()

    activePortfolio_df['Class Group'] = ''
    activePortfolio_df['Product Group'] = ''
    activePortfolio_df['Minimum Charge'] = ''
    activePortfolio_df['MM Adjustment'] = ''

    # list that contains symbols which arent in 'included products'
    unlistedSymbol = []

    for i in range(len(symbol)):
        found = False
        
        # for options
        if symbol[i].startswith("."):
            ticker = ""
            
            # Extract Symbol
            for char in symbol[i][1:]:
                if not char.isspace():
                    ticker += char
                else:
                    break
            
            # Extracts information from included products
            for j in range(0,len(productType)):
                if ticker == occSymbol[j] and productType[j] == 'OPTN':
                    found = True
                    activePortfolio_df.loc[i, "Class Group"] = prod_df.loc[j, "Class Group"]
                    activePortfolio_df.loc[i, "Product Group"] = prod_df.loc[j, "Product Group"]
                    activePortfolio_df.loc[i, "Minimum Charge"] = prod_df.loc[j, "Minimum Charge"]
                    activePortfolio_df.loc[i, 'MM Adjustment'] = prod_df.loc[j, 'Market Move Adjustment Factor']

        # for equities
        else:
            # Extracts information from included products
            for j in range(0,len(productType)):
                if symbol[i] == occSymbol[j] and productType[j] == 'EQUI':
                    found = True
                    activePortfolio_df.loc[i, "Class Group"] = prod_df.loc[j, "Class Group"]
                    activePortfolio_df.loc[i, "Product Group"] = prod_df.loc[j, "Product Group"]
                    activePortfolio_df.loc[i, "Minimum Charge"] = prod_df.loc[j, "Minimum Charge"]
                    activePortfolio_df.loc[i, 'MM Adjustment'] = prod_df.loc[j, 'Market Move Adjustment Factor']
        
        # assigning default values for symbols not found
        if not found:
            unlistedSymbol.append(symbol[i])
            activePortfolio_df.loc[i, "Class Group"] = symbol[i]
            activePortfolio_df.loc[i, "Product Group"] = 999
            activePortfolio_df.loc[i, "cpmUp"] = 15.0
            activePortfolio_df.loc[i, "cpmDown"] = 15.0
            activePortfolio_df.loc[i, "Product Group Offset"] = 0
            activePortfolio_df.loc[i, "Minimum Charge"] = 0
            activePortfolio_df.loc[i, 'MM Adjustment'] = 1
            if symbol[i].startswith("."):
                activePortfolio_df.loc[i, "Minimum Charge"] = 37.5

    # product data and stress test thresholds
    url = "https://marketdata.theocc.com/rbh-documentation?fileName=RBHCPMProductGroupsReport&format=csv"
    productData_df = pd.read_csv(url)
    productData_df = productData_df[:-1]
    productType = productData_df['Product Group'].tolist()

    activePortfolio_df['cpmUp'] = ''
    activePortfolio_df['cpmDown'] = ''
    activePortfolio_df['Product Group Offset'] = ''
    productTypePort = activePortfolio_df['Product Group'].tolist()

    for i in range(len(productTypePort)):
        for j in range(len(productType)):
            if productTypePort[i] == float(productType[j]):
                activePortfolio_df.loc[i, "cpmUp"] = productData_df.loc[j, "CPM % Up"]
                activePortfolio_df.loc[i, "cpmDown"] = productData_df.loc[j, "CPM % Down"]
                activePortfolio_df.loc[i, "Product Group Offset"] = productData_df.loc[j, "Product Group Offset %"]
                
    # converts expiration date to percentage of year for black scholes calculations
    expiration = activePortfolio_df['Expiration'].tolist()

    today = pd.to_datetime('today').normalize()
    fracYear = []
    for i in range(len(expiration)):
        if symbol[i].startswith("."):
            business_days = pd.bdate_range(today, expiration[i])
            fraction = float(len(business_days))/252
            fracYear.append(fraction)

        else:
            fracYear.append(None)
    activePortfolio_df['Expiration in Fraction'] = fracYear

    # call OCC min calculator function
    activePortfolio_df[["Market Value", "OCC Min"]] = activePortfolio_df.apply(lambda row: occ_min(row['symbol'], row['pos'], row['optionprice'],
                                                                                              row['underlyingprice'], row['Minimum Charge']), axis = 1)
    # call stress test function
    activePortfolio_df[["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"]] = activePortfolio_df.apply(lambda row: stress_test(row['pos'],
                                                                                                                    row['optionprice'],
                                                                                                                    row['underlyingprice'],
                                                                                                                                row['volatility'],
                                                                                                                                row['MM Adjustment'],
                                                                                                                    row['Expiration in Fraction'],
                                                                                                                    row['Call/Put'],
                                                                                                                    row['Strike'],
                                                                                                                        row['cpmUp'],
                                                                                                                    row['cpmDown']), axis = 1)
    
    #output estimated margin requirement per trade
    acct_instrument = activePortfolio_df.copy(deep = True)
    acct_instrument = acct_instrument.drop(["volatility", "Minimum Charge", "MM Adjustment", "cpmUp", "cpmDown", "Product Group Offset", "Expiration in Fraction"], axis=1)
    acct_instrument['House Requirement'] = acct_instrument.loc[:, ["OCC Min", "-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"]].min(axis=1, numeric_only=1)

    # output acct_instrument

    # aggregate stress tests and OCC min by 'Class Group'
    agg_functions = {'account': 'first','OCC Min': 'sum', '-5': 'sum', '-4': 'sum','-3': 'sum','-2': 'sum','-1': 'sum',
                    '1': 'sum','2': 'sum','3': 'sum','4': 'sum','5': 'sum'}

    acct_class = activePortfolio_df.groupby(['account', 'Product Group', 'Product Group Offset', 'Class Group'], sort=False, as_index=False).aggregate(agg_functions).reindex(columns=activePortfolio_df.columns).dropna(axis = 1)
    acct_class['House Requirement'] = acct_class.min(axis=1, numeric_only=1)
    acct_class = acct_class[acct_class['House Requirement'] < 0]
    acct_class = acct_class[['account', 'Class Group', 'House Requirement', 'OCC Min', '-5', '-4', '-3', '-2', '-1', '1', '2', '3', '4', '5', 'Product Group', 'Product Group Offset']]
    # pd.set_option('display.max_rows', None)

    # output acct_class

    agg_functions_firm = {'Class Group': 'first','House Requirement': 'sum', 'OCC Min': 'sum', '-5': 'sum', '-4': 'sum','-3': 'sum','-2': 'sum','-1': 'sum',
                 '1': 'sum','2': 'sum','3': 'sum','4': 'sum','5': 'sum'}

    firm_class = acct_class.groupby(['Class Group'], sort=False, as_index=False).aggregate(agg_functions_firm).reindex(columns=activePortfolio_df.columns).dropna(axis = 1)
    firm_class['House Requirement'] = firm_class.min(axis=1, numeric_only=1)
    firm_class = firm_class[firm_class['House Requirement'] < 0]
    firm_class = firm_class[['Class Group', 'House Requirement', 'OCC Min', '-5', '-4', '-3', '-2', '-1', '1', '2', '3', '4', '5']]

    #output firm_class

    firm_instrument = acct_instrument.groupby(['symbol'], sort=False, as_index=False).aggregate(agg_functions_firm).reindex(columns=activePortfolio_df.columns).dropna(axis = 1)
    firm_instrument['House Requirement'] = firm_instrument.min(axis=1, numeric_only=1)
    firm_instrument = firm_instrument[firm_instrument['House Requirement'] < 0]
    firm_instrument = firm_instrument[['symbol', 'Class Group', 'House Requirement', 'OCC Min', '-5', '-4', '-3', '-2', '-1', '1', '2', '3', '4', '5']]

    # output firm_instrument

    # offsetting for product groups
    agg_prod_df = acct_class.copy(deep = True)
    agg_prod_df.drop(['House Requirement'], axis=1)
    agg_prod_df[["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"]] = acct_class.apply(lambda row: offset(row['-5'],
                                                                                                                    row['-4'],
                                                                                                                    row['-3'],
                                                                                                                    row['-2'],
                                                                                                                    row['-1'],
                                                                                                                    row['1'],
                                                                                                                    row['2'],
                                                                                                                    row['3'],
                                                                                                                    row['4'],
                                                                                                                    row['5'], 
                                                                                                                    row['Product Group Offset']), 
                                                                                                                    axis = 1)
    agg_functions1 = {'Product Group': 'first','OCC Min': 'sum', '-5': 'sum', '-4': 'sum','-3': 'sum','-2': 'sum','-1': 'sum',
                    '1': 'sum','2': 'sum','3': 'sum','4': 'sum','5': 'sum'}

    firm_product = agg_prod_df.groupby(['Product Group'], sort=False, as_index=False).aggregate(agg_functions1).reindex(columns=activePortfolio_df.columns).dropna(axis = 1)
    agg_prod_indiv_df = agg_prod_df.groupby(['account', 'Product Group'], sort=False, as_index=False).aggregate(agg_functions).reindex(columns=activePortfolio_df.columns).dropna(axis = 1)

    firm_product['Product Offset House Requirement'] = firm_product.min(axis=1, numeric_only=1)
    agg_prod_indiv_df['Product Offset House Requirement'] = agg_prod_indiv_df.min(axis=1, numeric_only=1)

    firm_product = firm_product[['Product Group', 'Product Offset House Requirement', 'OCC Min', '-5', '-4', '-3', '-2', '-1', '1', '2', '3', '4', '5']]
    agg_prod_df = agg_prod_indiv_df[['account', 'Product Group', 'Product Offset House Requirement', 'OCC Min', '-5', '-4', '-3', '-2', '-1', '1', '2', '3', '4', '5']]

    # finding potential portfolio offsets
    productGroupWhole = firm_product['Product Group'].tolist()
    productGroupIndiv = agg_prod_indiv_df['Product Group'].tolist()

    firm_product['Portfolio Offset'] = ''
    firm_product['Portfolio Group'] = ''

    agg_prod_indiv_df['Portfolio Offset'] = ''
    agg_prod_indiv_df['Portfolio Group'] = ''

    url = "https://marketdata.theocc.com/rbh-documentation?fileName=RBHCPMPortfolioGroupsReport&format=csv"
    portfolio_data = pd.read_csv(url)
    portfolio_data = portfolio_data[:-1]
    productGroups = portfolio_data['Product Group'].tolist()

    for i in range(len(productGroupWhole)):
        found = False
        for j in range(len(productGroups)):
            if productGroupWhole[i] == float(productGroups[j]):
                found = True
                firm_product.loc[i, "Portfolio Offset"] = portfolio_data.loc[j, "Portfolio Group Offset %"]
                firm_product.loc[i, "Portfolio Group"] = portfolio_data.loc[j, "Portfolio Group"]
        if not found:
            firm_product.loc[i, "Portfolio Offset"] = 0
            firm_product.loc[i, "Portfolio Group"] = None

    for i in range(len(productGroupIndiv)):
        found = False
        for j in range(len(productGroups)):
            if productGroupIndiv[i] == float(productGroups[j]):
                found = True
                agg_prod_indiv_df.loc[i, "Portfolio Offset"] = portfolio_data.loc[j, "Portfolio Group Offset %"]
                agg_prod_indiv_df.loc[i, "Portfolio Group"] = portfolio_data.loc[j, "Portfolio Group"]
        if not found:
            agg_prod_indiv_df.loc[i, "Portfolio Offset"] = 0
            agg_prod_indiv_df.loc[i, "Portfolio Group"] = None
            
                
    firm_portfolio = firm_product.copy(deep = True)
    firm_product = firm_product.drop(firm_product[firm_product["Portfolio Offset"] != 0].index)
    firm_portfolio = firm_portfolio[(firm_portfolio['Portfolio Offset'] != 0)]

    #output firm_product

    # treating the whole portfolio as one account - used only for the total tims estimate
    firm_portfolio[["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"]] = firm_portfolio.apply(lambda row: offset(row['-5'],
                                                                                                                    row['-4'],
                                                                                                                    row['-3'],
                                                                                                                    row['-2'],
                                                                                                                    row['-1'],
                                                                                                                    row['1'],
                                                                                                                    row['2'],
                                                                                                                    row['3'],
                                                                                                                    row['4'],
                                                                                                                    row['5'], 
                                                                                                                    row['Portfolio Offset']), 
                                                                                                                    axis = 1)
    agg_functions3 = {'Portfolio Group': 'first','OCC Min': 'sum', '-5': 'sum', '-4': 'sum','-3': 'sum','-2': 'sum','-1': 'sum',
                    '1': 'sum','2': 'sum','3': 'sum','4': 'sum','5': 'sum'}

    firm_portfolio = firm_portfolio.groupby(['Portfolio Group'], sort=False, as_index=False).aggregate(agg_functions3).dropna(axis = 1)
    firm_portfolio['Portfolio Offset House Requirement'] = firm_portfolio.min(axis=1, numeric_only=1)
    firm_portfolio = firm_portfolio[['Portfolio Offset House Requirement', 'OCC Min', '-5', '-4', '-3', '-2', '-1', '1', '2', '3', '4', '5']]

    # output firm_portfolio

    # creating individual dataframes for product and portfolio offsets per account
    portfolio_group_indiv_df = agg_prod_indiv_df.copy(deep = True)

    agg_prod_indiv_df = agg_prod_indiv_df.drop(agg_prod_indiv_df[agg_prod_indiv_df["Portfolio Offset"] != 0].index)
    portfolio_group_indiv_df = portfolio_group_indiv_df[(portfolio_group_indiv_df['Portfolio Offset'] != 0)]

    # performing portfolio offsets per each account
    portfolio_group_indiv_df[["-5", "-4", "-3", "-2", "-1", "1", "2", "3", "4", "5"]] = portfolio_group_indiv_df.apply(lambda row: offset(row['-5'],
                                                                                                                    row['-4'],
                                                                                                                    row['-3'],
                                                                                                                    row['-2'],
                                                                                                                    row['-1'],
                                                                                                                    row['1'],
                                                                                                                    row['2'],
                                                                                                                    row['3'],
                                                                                                                    row['4'],
                                                                                                                    row['5'], 
                                                                                                                    row['Portfolio Offset']), 
                                                                                                                    axis = 1)
    agg_functions2 = {'account': 'first','Portfolio Group': 'first','OCC Min': 'sum', '-5': 'sum', '-4': 'sum','-3': 'sum','-2': 'sum','-1': 'sum',
                    '1': 'sum','2': 'sum','3': 'sum','4': 'sum','5': 'sum'}
    portfolio_group_indiv_df = portfolio_group_indiv_df.groupby(['account','Portfolio Group'], sort=False, as_index=False).aggregate(agg_functions2).dropna(axis = 1)
    portfolio_group_indiv_df['Portfolio Offset House Requirement'] = portfolio_group_indiv_df.min(axis=1, numeric_only=1)
    portfolio_group_indiv_df = portfolio_group_indiv_df[['account','Portfolio Group', 'Portfolio Offset House Requirement', 'OCC Min', '-5', '-4', '-3', '-2', '-1', '1', '2', '3', '4', '5']]

    # creating an aggregate dataframe for individual accounts that has product and portfolio offsets
    df1 = agg_prod_indiv_df.rename(columns={"Product Group": "Group", "Product Offset House Requirement": "House Requirement"})
    df1 = df1[['account','Group', 'House Requirement']]
    df2 = portfolio_group_indiv_df.rename(columns={"Portfolio Group": "Group", "Portfolio Offset House Requirement": "House Requirement"})
    df2 = df2[['account','Group', 'House Requirement']]

    indiv_agg_df = pd.concat([df1, df2])

    # House Requirements per Account - (Rougher estimate, as it does not factor in offsets between accounts)
    agg_functions = {'House Requirement': 'sum'}
    houseRequirements_df = indiv_agg_df.groupby(indiv_agg_df['account'], sort = False).aggregate(agg_functions)
    houseRequirements_df = abs(houseRequirements_df)
    # output houseRequirements_df
    print(houseRequirements_df.round(2))

    # Total House Requirement
    portfolioRequirementList = firm_portfolio["Portfolio Offset House Requirement"].tolist()
    productRequirementList = firm_product["Product Offset House Requirement"].tolist()

    totalHouseRequirement = 0

    for i in range(len(portfolioRequirementList)):
        totalHouseRequirement = totalHouseRequirement + portfolioRequirementList[i]
    for i in range(len(productRequirementList)):
        totalHouseRequirement = totalHouseRequirement + productRequirementList[i]

    totalHouseRequirement = round(-totalHouseRequirement, 2)

    #output estimated total tims requirement
    print("Estimated Total TIMS Requirement: $" f"{totalHouseRequirement:,}")
if __name__ == "__main__":
    main()