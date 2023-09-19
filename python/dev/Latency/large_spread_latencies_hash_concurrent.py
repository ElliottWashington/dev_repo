import csv
import numpy as np
import pandas as pd
from datetime import datetime
import psycopg2
import argparse
import os
from concurrent.futures import ThreadPoolExecutor


def run_query(query, conn):
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()

def run_queries1(date, target, sender):
    conn = psycopg2.connect(
        host="10.7.8.59",
        database="fixtransactions",
        user="scalp",
        password="QAtr@de442",
        port="5433"
    )

    query1 = f"""
    CREATE TEMP TABLE temp_table1 AS
    SELECT tag11, tag100, tag41, tag49 
    FROM fixmsg 
    WHERE tag52 >= TIMESTAMP '{date}'
    AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND tag56 = '{target}'
    and tag49 = '{sender}'
    AND (tag35 = 'AB');
    """

    query2 = f"""
    CREATE TEMP TABLE temp_table2 AS
    SELECT a.tag11, a.tag52 AS cancel_52, b.tag52 AS cancelack_52, b.tag41, a.tag100
    FROM fixmsg a
    JOIN fixmsg b ON a.tag11 = b.tag41
    WHERE a.tag52 >= TIMESTAMP '{date}'
    AND a.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND b.tag52 >= TIMESTAMP '{date}'
    AND b.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    and a.tag49 = '{sender}'
    and b.tag56 = '{sender}'
    and b.tag49 = '{target}'
    AND b.tag35 = '8'
    AND b.tag39 = '4'
    AND (a.tag35 = 'AB' or a.tag35 = 'D')
    AND a.tag56 = '{target}';
    """

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(run_query, query1, conn), executor.submit(run_query, query2, conn)]
        for future in futures:
            future.result()

    # Fetch results from the temp_table2
    fetch_query = "SELECT * FROM temp_table2;"
    with conn.cursor() as cursor:
        cursor.execute(fetch_query)
        result2 = cursor.fetchall()
        column_names2 = [desc[0] for desc in cursor.description]

    conn.close()

    hash_table = {}
    for row in result2:
        row_dict = {column_names2[i]: row[i] for i in range(len(column_names2))}
        hash_table[row_dict["tag11"]] = row_dict

    for key in hash_table.keys():
        hash_table[key]['OID'] = hash_table[key].pop('tag11')
        hash_table[key]['reply_OID'] = hash_table[key].pop('tag41')
        hash_table[key]['exchange'] = hash_table[key].pop('tag100')
        hash_table[key]['sent_time'] = hash_table[key].pop('cancel_52')
        hash_table[key]['reply_time'] = hash_table[key].pop('cancelack_52')
    return hash_table

def write_to_database(date):
    # Construct the filename of the CSV file to read from
    filename = f"/home/elliott/development/files/spread_cancelack_INCAPNS_2%highest_latencies_{date}_summary.csv"

    # Connect to the database
    conn = psycopg2.connect(
        host="10.7.8.59",
        database="reports",
        user="scalp",
        password="QAtr@de442",
        port="5433"
    )

    # Open the CSV file
    with open(filename, "r") as f:
        reader = csv.reader(f)
        next(reader)  # Skip the header row

        # Loop through the rows and insert them into the database
        for row in reader:
            exchange = row[0]
            average_time_difference = row[1]
            counts = row[2]

            cur = conn.cursor()
            cur.execute(
                "INSERT INTO average_latency_spread (date, exchange, count, average_difference) VALUES (%s, %s, %s, %s)",
                (date, exchange, counts, average_time_difference)
            )

            conn.commit()

    # Close the database connection
    conn.close()

def main():
    parser = argparse.ArgumentParser(description='Run queries and analysis for equity latencies.')
    parser.add_argument('date', help='Date for the queries (YYYY-MM-DD format)')
    parser.add_argument('target', help='Target for the queries')
    parser.add_argument('sender', help='Sender for the queries')

    args = parser.parse_args()

    date = args.date
    target = args.target
    sender = args.sender

    hash_table_df = run_queries1(date, target, sender)
    
    table2_data = list(hash_table_df.values())
    counts = {}
    for row in table2_data:
        exchange = row['exchange']
        if exchange in counts:
            counts[exchange] += 1
        else:
            counts[exchange] = 1

    result_data = []
    for row in table2_data:
        time_difference = (row['reply_time'] - row['sent_time']).total_seconds()
        exchange = row['exchange']
        count = counts[exchange]
        result_data.append({
            'OID': row['OID'],
            'sent_time': row['sent_time'],
            'reply_time': row['reply_time'],
            'reply_OID': row['reply_OID'],
            'exchange': exchange,
            'time_difference': time_difference,
            'counts': count
        })
    result_data = sorted(result_data, key=lambda x: x['time_difference'], reverse=True)[:4000]
    all_data = sorted(result_data, key=lambda x: x['time_difference'], reverse=True)

    first_csv_file = f'/home/elliott/development/files/spread_cancelack_{target}_2%highest_latencies_{date}.csv'
    with open(first_csv_file, 'w') as file:
        fieldnames = ['OID', 'sent_time', 'reply_time', 'reply_OID', 'exchange', 'time_difference', 'counts']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(result_data)

    # Read the first CSV file
    first_csv_data = pd.DataFrame(all_data)

    # Compute the grouped data by exchange and average the time_difference
    grouped_data = first_csv_data.groupby(['exchange']).agg({'time_difference': 'mean', 'counts': 'sum'}).reset_index()
    grouped_data['date'] = date

    # Write the grouped data to a CSV file
    grouped_data.to_csv(f'/home/elliott/development/files/spread_cancelack_{target}_2%highest_latencies_{date}_summary.csv', index=False)
    
    write_to_database(date)

if __name__ == '__main__':
    main()  