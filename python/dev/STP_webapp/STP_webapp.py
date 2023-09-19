from flask import Flask, render_template, request
import psycopg2
import psycopg2.extras
from datetime import datetime
import matplotlib.pyplot as plt
import os
from io import BytesIO
import base64
from bokeh.plotting import figure
from bokeh.embed import components
from bokeh.models import HoverTool


app = Flask(__name__)

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

def plot_vols(results):
    if results:
        mnn_values = [result['mnn'] for result in results]
        vol_values = [result['vol'] for result in results]
        volm_values = [result['volm'] for result in results]

        # create a new plot with a title and axis labels
        p = figure(title="Moneyness vs Volatility", x_axis_label='Moneyness', y_axis_label='Volatility', 
                   tools="pan,wheel_zoom,xbox_select,reset", 
                   background_fill_color="#fafafa", width=800, height=600)
        
        # add a line renderer with legend and line thickness for Vol
        vol_line = p.line('x', 'y', source = {'x': mnn_values, 'y': vol_values}, color="navy", alpha=0.5, legend_label="Vol")

        # add a line renderer with legend and line thickness for VolM
        volm_line = p.line('x', 'y', source = {'x': mnn_values, 'y': volm_values}, color="firebrick", alpha=0.5, legend_label="VolM")

        # Add a hover tool referring to the formatted columns
        vol_hover = HoverTool(renderers=[vol_line],
                             tooltips = [("Moneyness", "@x"),
                                         ("Vol", "@y")])
        volm_hover = HoverTool(renderers=[volm_line],
                              tooltips = [("Moneyness", "@x"),
                                          ("VolM", "@y")])
        p.add_tools(vol_hover, volm_hover)

        # generate HTML components
        script, div = components(p)

        return script, div
    return None, None

def db_query(query, conn_number, format_cols=None):
    if conn_number == "1":
        conn = psycopg2.connect(host='10.7.8.59', database='fixtransactions', user='scalp', password='QAtr@de442', port='5432')
    else:
        conn = psycopg2.connect(host='10.7.8.59', database='theoretical', user='market_data_ro', password='scalp2022', port='5433')    
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(query)
    raw_results = cur.fetchall()
    cur.close()
    conn.close()

    # Post-processing step to handle zero values
    results = []
    for row in raw_results:
        result = dict(row)
        if format_cols:
            for col, format_spec in format_cols.items():
                result[col] = format(result[col], format_spec)
        results.append(result)

    return results


def db_query_account_numbers():
    query = "SELECT DISTINCT account_number FROM positions where account_number like '%P%'"
    account_number_dicts = db_query(query, "1", format_cols=None)
    account_numbers = [d['account_number'] for d in account_number_dicts]
    return account_numbers


@app.route('/trading', methods=['GET', 'POST'])
def trading():
    results = []
    internal_accounts = list(mapping.values())
    if request.method == 'POST':
        trading_date = request.form.get('trading_date')
        symbol = request.form.get('symbol')
        selected_internal_account = request.form.get('clearing_account')

        query = f"SELECT trade_timestamp, cl_order_id, clearing_account, symbol, quantity, price, broker_id, trade_type_id, exec_id, trading_date FROM trades WHERE trading_date = '{trading_date}'"

        if symbol:
            query += f" AND symbol like '{symbol}%'"

        if selected_internal_account and selected_internal_account != 'all':
            selected_clearing_account = next((k for k, v in mapping.items() if v == selected_internal_account), None)

            if selected_clearing_account:
                query += f" AND clearing_account = '{selected_clearing_account}'"
        
        query += " ORDER BY trade_timestamp DESC"
        
        results = db_query(query, "1", {'quantity': '.2f', 'price': '.2f'})

        # Convert clearing_account to internal_account for displaying in the table
        for result in results:
            if result['clearing_account'] in mapping:
                result['clearing_account'] = mapping[result['clearing_account']]

    return render_template('trading.html', results=results, accounts=internal_accounts)

@app.route('/positions', methods=['GET', 'POST'])
def positions():
    results = []
    account_numbers = db_query_account_numbers()
    if request.method == 'POST':
        sod_date = request.form.get('sod_date')
        symbol = request.form.get('symbol')
        selected_account_number = request.form.get('account_number')

        query = f"SELECT * FROM public.positions WHERE sod_date = '{sod_date}'"

        if symbol:
            query += f" AND symbol = '{symbol}'"
        if selected_account_number and selected_account_number != 'all':
            query += f" AND account_number = '{selected_account_number}'"
        query += " ORDER BY sod_date DESC"
        
        results = db_query(query, "1", {'quantity': '.2f', 'average_cost': '.2f'})

    return render_template('positions.html', results=results, accounts=account_numbers)

@app.route('/sod_positions', methods=['GET', 'POST'])
def sod_positions():
    results = []
    account_numbers = db_query_account_numbers()
    if request.method == 'POST':
        sod_date = request.form.get('sod_date')
        symbol = request.form.get('symbol')
        selected_account_number = request.form.get('account_number')

        query = f"SELECT sod_date, average_cost, symbol, quantity, account_number, clearing_account_number FROM public.positions_daily_s WHERE sod_date = '{sod_date}'"

        if symbol:
            query += f" AND symbol = '{symbol}'"
        if selected_account_number and selected_account_number != 'all':
            query += f" AND account_number = '{selected_account_number}'"
        query += " ORDER BY sod_date DESC"
        
        results = db_query(query, "1", {'quantity': '.2f', 'average_cost': '.2f'})

    return render_template('sod_positions.html', results=results, accounts=account_numbers)

@app.route('/implied_vols', methods=['GET', 'POST'])
def implied_vols():
    results = []
    plot_script = None
    plot_div = None
    if request.method == 'POST':
        symbol = request.form.get('symbol')
        query = f"""
                SELECT distinct on (a.strike_in) a.strike_in, a.mnn, a.vol, a.volm,
                a.stock_p, a.tte_in,
                a.cbp, a.cap, b.pbp, b.pap,
                a.rate, a.mnn
                FROM
                (SELECT distinct on (strike_in) strike_in, v.imp_vol_c_out as vol, v.imp_vol_c_mdl as volm, (v.stock_bbo_bid_price + v.stock_bbo_ask_price)/2.0 as stock_p, v.tte_in,
                v.rate_in as rate, v.option_bid_price as cbp, v.option_ask_price as cap, v.mnn as mnn
                FROM public.theo_implied_vols v, public.theo_opt_sym_chain s
                WHERE v.theo_symbol_id = s.theo_symbol_id 
                and symbol like '{symbol}%'
                order by strike_in, v.create_time desc) a
                JOIN
                (SELECT distinct on(strike_in ) strike_in, v.option_bid_price as pbp, v.option_ask_price as pap
                FROM public.theo_implied_vols v, public.theo_opt_sym_chain s
                WHERE v.theo_symbol_id = s.theo_symbol_id 
                and symbol like '{symbol}%'
                order by strike_in, v.create_time desc ) b
                ON a.strike_in = b.strike_in
                order by strike_in asc
        """        
        results = db_query(query, "2", format_cols=None)
        plot_script, plot_div = plot_vols(results)
        
    return render_template('implied_vols.html', results=results, plot_script=plot_script, plot_div=plot_div)

@app.route('/latest_rates', methods=['GET', 'POST'])
def latest_rates():
    results = []
    if request.method == 'POST':
        symbol = request.form.get('symbol')
        query = f"""
                SELECT distinct on (s.strike) s.strike, r.theo_rate as rate, 0.01 * r.stkmnn as mnn
                FROM public.theo_rates r, public.theo_opt_sym_chain s
                WHERE r.theo_symbol_id = s.theo_symbol_id 
                and symbol like '{symbol}%'
                order by s.strike, r.create_time desc
            """        
        results = db_query(query, "2", format_cols=None)
        print(query)
        print(results)

    return render_template('latest_rates.html', results=results)

@app.route('/')
def home():
    return render_template('home.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
