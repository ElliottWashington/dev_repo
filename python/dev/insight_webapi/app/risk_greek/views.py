from flask import Blueprint, render_template, request
from app.risk_greek.utils import db_query, mapping

risk_greek_bp = Blueprint('risk_greek', __name__)

@risk_greek_bp.route('/risk_greek', methods=['GET', 'POST'])
def risk_greek():
    results = []
    internal_accounts = list(mapping.values())
    if request.method == 'POST':
        timestamp = request.form.get('timestamp')
        symbol = request.form.get('symbol')
        selected_internal_account = request.form.get('account')  # assuming you want to filter by account

        query = f"SELECT * FROM greek_data WHERE timestamp::date = '{timestamp}'"

        if symbol:
            query += f" AND symbol like '%{symbol}%'"

        if selected_internal_account and selected_internal_account != 'all':
            selected_clearing_account = next((k for k, v in mapping.items() if v == selected_internal_account), None)

            if selected_clearing_account:
                query += f" AND account = '{selected_clearing_account}'"
        
        query += " ORDER BY timestamp DESC"
        print(query)
        results = db_query(query, "2", None)  # Assuming connection number 2 for the risk_greek table

        # Convert account to internal_account for displaying in the table
        for result in results:
            if result['account'] in mapping:
                result['account'] = mapping[result['account']]

    return render_template('risk_greek.html', results=results, accounts=internal_accounts)