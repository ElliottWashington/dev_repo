import decimal
from decimal import Decimal
from collections import defaultdict
import psycopg2
import pandas as pd
import csv
import argparse

# Database connection credentials
db_credentials = {
    'dbname': 'fixtransactions',
    'user': 'scalp',
    'password': 'QAtr@de442',
    'host': '10.7.8.59',
    'port': '5433'
}
def process_trades(conn, date):
    traders_data = defaultdict(lambda: defaultdict(int))

    # Create a named server-side cursor
    with conn.cursor(name='server_side_cursor') as cursor:
        query = f"""SELECT tag1, tag35, tag39, tag150, tag38, tag32,tag167
                    FROM fixmsg
                    WHERE tag1 IS NOT null
                    AND tag35 IS NOT null
                    AND tag39 IS NOT null
                    AND tag150 IS NOT null
                    AND tag38 IS NOT null
                    AND tag167 IS NOT null
                    AND tag32 IS NOT null
                    AND tag52 >= TIMESTAMP '{date} 07:40:00'
                    AND tag52 < TIMESTAMP '{date} 16:15:00';
                """
        cursor.execute(query)

        # Fetch and process rows one at a time
        batch_size = 1000000
        while True:
            trades = cursor.fetchmany(batch_size)
            if not trades:
                break

            for trade in trades:
                account = trade[0]
                msg_type = trade[1]
                order_status = trade[2]
                exec_type = trade[3]
                order_qty = trade[4]
                last_shares = trade[5]
                instrument = trade[6]


                # Count the number of messages sent by each account
                if msg_type == '8' and order_status == '0':
                    traders_data[account]['messages'] += 1
                else:
                    traders_data[account]['messages'] += 0
                
                # Count the number of orders executed by each account
                if msg_type == '8' and order_status == '2':
                    traders_data[account]['orders_executed'] += 1
                else:
                    traders_data[account]['orders_executed'] += 0
                
                # Count the number of orders cancelled by each account
                if msg_type == '8' and order_status == '4':
                    traders_data[account]['orders_canceled'] += 1
                else:
                    traders_data[account]['orders_canceled'] += 0

                # Count the number of equity-specific shares submitted by each account
                if msg_type == '8' and instrument == 'CS' and order_status == '0':
                    traders_data[account]['equity_shares_submitted'] += order_qty
                else:
                    traders_data[account]['equity_shares_submitted'] += 0

                # Count the number of equity-specific shares executed by each account
                if msg_type == '8' and instrument == 'CS' and order_status == '2':
                    traders_data[account]['equity_shares_executed'] += order_qty
                else:
                    traders_data[account]['equity_shares_executed'] += 0
                
                # Count the number of equity-specific shares submitted by each account
                if msg_type == '8' and instrument == 'CS' and order_status == '4':
                    traders_data[account]['equity_shares_canceled'] += order_qty
                else:
                    traders_data[account]['equity_shares_canceled'] += 0

                if msg_type == '8' and instrument == 'OPT' and order_status == '0':
                    traders_data[account]['option_shares_submitted'] += order_qty
                else:
                    traders_data[account]['option_shares_submitted'] += 0

                # Count the number of equity-specific shares executed by each account
                if msg_type == '8' and instrument == 'OPT' and order_status == '2':
                    traders_data[account]['option_shares_executed'] += order_qty
                else:
                    traders_data[account]['option_shares_executed'] += 0

                if msg_type == '8' and instrument == 'OPT' and order_status == '4':
                    traders_data[account]['option_shares_canceled'] += order_qty
                else:
                    traders_data[account]['option_shares_canceled'] += 0
    
    

    total_orders = sum([data['orders_executed'] + data['orders_canceled'] for account, data in traders_data.items()])
    total_executed = sum([data['orders_executed'] for account, data in traders_data.items()])
    total_canceled = sum([data['orders_canceled'] for account, data in traders_data.items()])

    total_equities_submitted = sum([data['equity_shares_submitted'] for account, data in traders_data.items()])
    total_equities_executed = sum([data['equity_shares_executed'] for account, data in traders_data.items()])
    total_equities_canceled = sum([data['equity_shares_canceled'] for account, data in traders_data.items()])

    total_options_submitted = sum([data['option_shares_submitted'] for account, data in traders_data.items()])
    total_options_executed = sum([data['option_shares_executed'] for account, data in traders_data.items()])
    total_options_canceled = sum([data['option_shares_canceled'] for account, data in traders_data.items()])

    percentage_executed = (total_executed / total_orders) * 100
    percentage_canceled = (total_canceled / total_orders) * 100
    percentage_equities_canceled = (total_equities_canceled / total_equities_submitted) * 100
    percentage_equities_executed = (total_equities_executed / total_equities_submitted) * 100
    percentage_options_canceled = (total_options_canceled / total_options_submitted) * 100
    percentage_options_executed = (total_options_executed / total_options_submitted) * 100

    return traders_data, total_orders, total_executed, total_canceled, total_equities_submitted, total_equities_executed, total_equities_canceled, total_options_submitted, total_options_executed, total_options_canceled, percentage_executed, percentage_canceled, percentage_equities_executed, percentage_equities_canceled, percentage_options_executed, percentage_options_canceled

def main(date):
    conn = psycopg2.connect(**db_credentials)
    traders_data, total_orders, total_executed, total_canceled, total_equities_submitted, total_equities_executed, total_equities_canceled, total_options_submitted, total_options_executed, total_options_canceled, percentage_executed, percentage_canceled, percentage_equities_executed, percentage_equities_canceled, percentage_options_executed, percentage_options_canceled = process_trades(conn, date)
    conn.close()

    
    header_strings = [
        f"total_orders: {total_orders}, total_executed: {total_executed}, total_canceled: {total_canceled}",
        f"total_equities_submitted: {total_equities_submitted}, total_equities_executed: {total_equities_executed}, total_equities_canceled: {total_equities_canceled}",
        f"total_options_submitted: {total_options_submitted}, total_options_executed: {total_options_executed}, total_options_canceled: {total_options_canceled}",
        f"percentage_executed: {percentage_executed:.1f}, percentage_equities_executed: {percentage_equities_executed:.1f}, percentage_options_executed: {percentage_options_executed:.1f}",
        f"percentage_canceled: {percentage_canceled:.1f}, percentage_equities_canceled: {percentage_equities_canceled:.1f}, percentage_options_canceled: {percentage_options_canceled:.1f}"
    ]

    with open(f'/home/elliott/Development/files/execution_ratio_{date}.csv', mode='w') as csv_file:
        writer = csv.writer(csv_file)
        
        # Write header strings to the CSV file
        for header in header_strings:
            writer.writerow([header])
        
        # Write column names to the CSV file
        writer.writerow(['Account', 'Messages', 'Orders Executed', 'Orders Cancelled', 
                        'Equity Shares Submitted', 'Equity Shares Executed', 'Equity Shares Cancelled',
                        'Option Shares Submitted', 'Option Shares Executed', 'Option Shares Cancelled'])
        
        # Write data to the CSV file
        for account, data in traders_data.items():
            writer.writerow([account, data['messages'], data['orders_executed'], data['orders_canceled'],
                            data['equity_shares_submitted'], data['equity_shares_executed'], data['equity_shares_canceled'],
                            data['option_shares_submitted'], data['option_shares_executed'], data['option_shares_canceled']])


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run queries for a given date")
    parser.add_argument("date", type=str, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    # Call the main function with the date argument
    main(args.date)

