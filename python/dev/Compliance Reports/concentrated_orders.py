import psycopg2
import argparse
from collections import defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

def non_options_positions_for_trader(tag1, date):
    connection = psycopg2.connect(host="10.7.8.59", database="fixtransactions", user="scalp", password="QAtr@de442", port='5433')
    cursor = connection.cursor()

    query = f"""
    SELECT tag55, tag54, tag38, tag39
    FROM fixmsg
    WHERE tag52 >= TIMESTAMP '{date}'
    AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 day'
    AND tag1 = '{tag1}'
    AND (tag39 = '1' OR tag39 = '2') 
    AND tag167 != 'OPT';
    """
    cursor.execute(query)

    positions = defaultdict(float)
    batch_size = 1000000

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break

        for tag55, tag54, tag38, tag39 in rows:
            if tag39 in ('1', '2'):
                positions[tag55] += float(tag38) * (1 if tag54 == '1' else -1)

    cursor.close()
    connection.close()

    return tag1, positions

def options_positions(tag1, date):
    connection = psycopg2.connect(host="10.7.8.59", database="fixtransactions", user="scalp", password="QAtr@de442", port='5433')
    cursor = connection.cursor()

    query = f"""
    SELECT tag55, tag54, tag38, tag52, tag200, tag201, tag202, tag205, tag31, tag39
    FROM fixmsg
    WHERE tag52 >= TIMESTAMP '{date}'
    AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 day'
    AND tag1 = '{tag1}'
    AND tag167 = 'OPT'
    AND tag200 IS NOT NULL
    AND tag201 IS NOT NULL
    AND tag202 IS NOT NULL
    AND tag205 IS NOT NULL;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    record_count = len(rows)

    positions_before_313 = defaultdict(float)
    orders_313_to_315 = []

    closing_time = datetime.strptime(f'{date} 16:15:00', '%Y-%m-%d %H:%M:%S')
    time_window_start = closing_time - timedelta(minutes=6)

    for tag55, tag54, tag38, tag52, tag200, tag201, tag202, tag205, tag31, tag39 in rows:
        option_type = 'P' if tag201 == '0' else 'C'
        expiration_date = datetime.strptime(f"{tag200}{tag205}", '%Y%m%d').strftime('%Y%m%d')
        strike_price = f"{float(tag202):.0f}"
        option_contract_name = f"{tag55}{expiration_date}{option_type}{strike_price}"

        if tag52 < time_window_start:
            if tag39 == '2':
                positions_before_313[option_contract_name] += float(tag38) * (1 if tag54 == '1' else -1)
        elif time_window_start <= tag52 < closing_time:
            if option_contract_name in positions_before_313 and tag39 != '2':
                open_position = positions_before_313[option_contract_name] * (1 if tag54 == '1' else -1)
                if open_position != 0:
                    orders_313_to_315.append((option_contract_name, tag54, tag38, tag52))

    cursor.close()
    connection.close()

    return tag1, positions_before_313, orders_313_to_315, record_count

def all_positions(date, traders):
    with ThreadPoolExecutor() as executor:
        non_option_futures = [executor.submit(non_options_positions_for_trader, trader, date) for trader in traders]
        option_futures = [executor.submit(options_positions, trader, date) for trader in traders]
        
        non_option_results = [future.result() for future in non_option_futures]
        option_results = [future.result() for future in option_futures]

    non_option_positions_per_trader = {tag1: positions for tag1, positions in non_option_results}
    option_positions_per_trader = {tag1: positions for tag1, positions, _, _ in option_results}

    return non_option_positions_per_trader, option_positions_per_trader

traders = ['5P525705', '31P532753', '31P532753','5646353','1P525501','5647492','12P52852','14P52874','3P528403','8P528918','24P531126','15P52595','34P534728',
    '20P529221','3618588','17P52867','30P532652','6P528806','22P530624','26P531527','3616407','7P526007','23P530525','19P529523','25P531426','4P525604',
    '32P534563','33P534612','16P52836','2P525802','27P532248']


def run(date):
    non_option_trader_positions, option_trader_positions = all_positions(date, traders)

    for trader, positions in non_option_trader_positions.items():
        if positions:
            print(f"Trader: {trader}")
            for symbol, position in positions.items():
                if position != 0:
                    print(f"{symbol}: {position}")
            print()

    for trader, positions in option_trader_positions.items():
        if positions:
            print(f"Trader: {trader}")
            for symbol, position in positions.items():
                if position != 0:
                    print(f"{symbol}: {position}")
            print()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add latency data to the database")
    parser.add_argument("date", help="the date to use for the latency data (format: yyyy-mm-dd)")
    args = parser.parse_args()
    
    run(args.date)
