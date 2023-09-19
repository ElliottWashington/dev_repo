from flask import Blueprint, render_template, request
from app.trading.utils import db_query, db_query_account_numbers

positions_bp = Blueprint('positions', __name__)

@positions_bp.route('/positions', methods=['GET', 'POST'])
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