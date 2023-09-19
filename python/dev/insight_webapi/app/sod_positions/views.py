from flask import Blueprint, render_template, request
from app.trading.utils import db_query, db_query_account_numbers

sod_positions_bp = Blueprint('sod_positions', __name__)

@sod_positions_bp.route('/sod_positions', methods=['GET', 'POST'])
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