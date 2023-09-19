from flask import Blueprint, render_template, request
from app.implied_vols.utils import db_query, plot_vols

implied_vols_bp = Blueprint('implied_vols', __name__)

@implied_vols_bp.route('/implied_vols', methods=['GET', 'POST'])
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