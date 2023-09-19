import psycopg2
from collections import defaultdict
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import argparse

def compute_positions_and_orders(tag1, date):
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

    closing_time = datetime.strptime('2023-04-04 16:15:00', '%Y-%m-%d %H:%M:%S')
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

def run_report(output_file="output.txt"):

    unfilled_orders = []

    with open(output_file, "w") as f:
        for result in results:
            print("", file=f)
            tag1, positions_before_313, orders_313_to_315, record_count = result
            print(f"Trader {tag1}: {record_count} records processed", file=f)
            print("  Positions before 3:13 PM EST:", file=f)
            for option_contract_name, tag38 in positions_before_313.items():
                if tag38 != 0:
                    print(f"    {option_contract_name}: {tag38}", file=f)
            unfilled_orders += [(tag1, *order) for order in orders_313_to_315]

        unfilled_orders = sorted(unfilled_orders, key=lambda x: x[3], reverse=True)
        print("", file=f)
        print("╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗", file=f)
        print("", file=f)
        print("Unfilled Orders between 3:13 PM and 3:15 PM EST:", file=f)
        for order in unfilled_orders:
            tag1, option_contract_name, tag54, tag38, tag52 = order
            action = "To Buy" if tag54 == "1" else "To Sell"
            if tag38 != 0:
                print(f"  Trader {tag1}: {action} {tag38} of {option_contract_name} at {tag52}", file=f)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute execution ratios for traders")
    parser.add_argument("date", help="Date in YYYY-MM-DD format", type=str)
    args = parser.parse_args()
    date = args.date

    traders = ['5P525705', '31P532753','5646353','1P525501','5647492','12P52852','14P52874','3P528403','8P528918','24P531126','15P52595','34P534728',
    '20P529221','3618588','17P52867','30P532652','6P528806','22P530624','26P531527','3616407','7P526007','23P530525','19P529523','25P531426','4P525604',
    '32P534563','33P534612','16P52836','2P525802','27P532248']

    with ThreadPoolExecutor() as executor:
        results = list(executor.map(compute_positions_and_orders, traders, [date] * len(traders)))

    run_report(f"/home/elliott/Development/files/price_on_close_{date}.txt")