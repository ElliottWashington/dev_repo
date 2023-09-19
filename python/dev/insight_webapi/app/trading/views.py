from flask import Blueprint, render_template, request
from app.trading.utils import db_query, mapping

trading_bp = Blueprint('trading', __name__)

@trading_bp.route('/trading', methods=['GET', 'POST'])
def trading():
    results = []
    internal_accounts = list(mapping.values())
    if request.method == 'POST':
        trading_date = request.form.get('trading_date')
        symbol = request.form.get('symbol')
        selected_internal_account = request.form.get('account')

        query = f"SELECT trade_timestamp, cl_order_id, clearing_account, symbol, quantity, price, broker_id, trade_type_id, exec_id, trading_date FROM trades WHERE trading_date = '{trading_date}'"

        if symbol:
            query += f" AND symbol like '{symbol}%'"

        if selected_internal_account and selected_internal_account != 'all':
            print(f"Selected internal account from form: {selected_internal_account}")
            selected_clearing_account = next((k for k, v in mapping.items() if v == selected_internal_account), None)
            print(f"Selected clearing account: {selected_clearing_account}")

            if selected_clearing_account:
                query += f" AND clearing_account = '{selected_clearing_account}'"
        
        query += " ORDER BY trade_timestamp DESC"
        
        results = db_query(query, "1", {'quantity': '.2f', 'price': '.2f'})

        # Convert clearing_account to internal_account for displaying in the table
        for result in results:
            if result['clearing_account'] in mapping:
                result['clearing_account'] = mapping[result['clearing_account']]

    return render_template('trading.html', results=results, accounts=internal_accounts)
