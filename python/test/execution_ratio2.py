import argparse
import csv
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import psycopg2

# Database connection credentials
db_credentials = {
    'dbname': 'fixtransactions',
    'user': 'scalp',
    'password': 'QAtr@de442',
    'host': '10.7.8.59',
    'port': '5433'
}

def process_trade(trade):
    account, msg_type, order_status, exec_type, order_qty, last_shares, instrument = trade
    trade_data = defaultdict(int)

    # Count the number of messages sent by each account
    if msg_type == '8' and order_status == '0':
        trade_data['messages'] += 1

    # Count the number of orders executed by each account
    if msg_type == '8' and order_status == '2':
        trade_data['orders_executed'] += 1

    # Count the number of orders canceled by each account
    if msg_type == '8' and order_status == '4':
        trade_data['orders_canceled'] += 1

    # Count the number of equity-specific shares submitted by each account
    if msg_type == '8' and instrument == 'CS' and order_status == '0':
        trade_data['equity_shares_submitted'] += order_qty

    # Count the number of equity-specific shares executed by each account
    if msg_type == '8' and instrument == 'CS' and order_status == '2':
        trade_data['equity_shares_executed'] += order_qty

    # Count the number of equity-specific shares canceled by each account
    if msg_type == '8' and instrument == 'CS' and order_status == '4':
        trade_data['equity_shares_canceled'] += order_qty

    if msg_type == '8' and instrument == 'OPT' and order_status == '0':
        trade_data['option_shares_submitted'] += order_qty

    # Count the number of equity-specific shares executed by each account
    if msg_type == '8' and instrument == 'OPT' and order_status == '2':
        trade_data['option_shares_executed'] += order_qty

    if msg_type == '8' and instrument == 'OPT' and order_status == '4':
        trade_data['option_shares_canceled'] += order_qty

    return account, trade_data

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
                    AND tag52 >= TIMESTAMP '{date}'
                    AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY';
                """
        cursor.execute(query)

        # Fetch and process rows in parallel using ThreadPoolExecutor
        batch_size = 1000000
        while True:
            trades = cursor.fetchmany(batch_size)
            if not trades:
                break

            with ThreadPoolExecutor() as executor:
                results = executor.map(process_trade, trades)
                for account, trade_data in results:
                    for key, value in trade_data.items():
                        traders_data[account][key] += value

    return traders_data

def write_to_csv(traders_data, output_file):
    with open(output_file, 'w', newline='') as csvfile:
        fieldnames = ['account', 'messages', 'orders_executed', 'orders_canceled',
                      'equity_shares_submitted', 'equity_shares_executed', 'equity_shares_canceled',
                      'option_shares_submitted', 'option_shares_executed', 'option_shares_canceled']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for account, data in traders_data.items():
            row = {'account': account}
            row.update(data)
            writer.writerow(row)

def main(date):
    # Connect to the database
    conn = psycopg2.connect(**db_credentials)

    # Process trades for the specified date
    traders_data = process_trades(conn, date)

    # Close the database connection
    conn.close()

    # Write the results to a CSV file
    write_to_csv(traders_data, output_file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process trades data.')
    parser.add_argument('date', type=str, help='Date for which to process trades (YYYY-MM-DD)')
    args = parser.parse_args()

    output_file = f"~/Development/files/execution_ratio_{args.date}.csv"
    main(args.date)
