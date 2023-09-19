from flask import Blueprint, render_template, request
from app.ppa.utils import db_query

ppa_bp = Blueprint('ppa', __name__)

@ppa_bp.route('/ppa', methods=['GET', 'POST'])
def latest_rates():
    results = []
    if request.method == 'POST':
        symbol = request.form.get('symbol')
        query = f"""
                SELECT distinct on(s.strike) strike, opt_bid_price, opt_ppa_bidp,
                opt_ppa_askp,  opt_ask_price,  theo_rate 
                FROM public.theo_rates r, public.theo_opt_sym_chain s
                WHERE r.theo_symbol_id = s.theo_symbol_id 
                and symbol like '{symbol}%'
                order by s.strike, r.create_time desc
            """        
        results = db_query(query, "2", format_cols=None)

    return render_template('ppa.html', results=results)