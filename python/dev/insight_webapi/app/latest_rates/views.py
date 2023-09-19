from flask import Blueprint, render_template, request
from app.latest_rates.utils import db_query

latest_rates_bp = Blueprint('latest_rate', __name__)

@latest_rates_bp.route('/latest_rates', methods=['GET', 'POST'])
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