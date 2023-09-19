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

def run_queries(date):
    conn = psycopg2.connect(
        host="10.7.8.59",
        database="fixtransactions",
        user="scalp",
        password="QAtr@de442",
        port="5433"
    )

    query1 = f"""
    CREATE TEMP TABLE outgoing AS
    SELECT 
        tag167,
        tag55,
        tag11,
        tag49,
        tag56,
        tag52,
        tag100
    FROM fixmsg
    WHERE 
        tag35 = 'D'
        AND tag167 = 'OPT'
        AND tag52 >= TIMESTAMP '{date}'
        AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    """

    query2 = f"""
    CREATE TEMP TABLE incoming AS
    SELECT 
        tag167,
        tag55,
        tag11,
        tag49,
        tag56,
        tag52,
        tag100
    FROM fixmsg
    WHERE 
        tag35 = '8'
        AND tag39 = '0'
        AND tag52 >= TIMESTAMP '{date}'
        AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    """

    query3 = f"""
    CREATE TEMP TABLE results AS
    SELECT 
        i.tag11 AS tag11,
        i.tag52 AS cancelack_52,
        i.tag100 AS tag100,
        o.tag11 AS tag11_a,
        o.tag56 AS target_compid,
        o.tag52 AS cancel_52
    FROM incoming i
    INNER JOIN outgoing o
        ON o.tag11 = i.tag11;
    """

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(run_query, query1, conn), executor.submit(run_query, query2, conn), executor.submit(run_query, query3, conn)]
        for future in futures:
            future.result()

    fetch_query = "SELECT * FROM results;"
    with conn.cursor() as cursor:
        cursor.execute(fetch_query)
        result_data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

    conn.close()

    hash_table = {}
    for row in result_data:
        row_dict = {column_names[i]: row[i] for i in range(len(column_names))}
        hash_table[row_dict["tag11"]] = row_dict

    return hash_table

def main():
    parser = argparse.ArgumentParser(description='Run queries and analysis for equity latencies.')
    parser.add_argument('date', help='Date for the queries (YYYY-MM-DD format)')
    args = parser.parse_args()
    date = args.date

    hash_table_df = run_queries(date)
    
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

    first_csv_file = f'/home/elliott/development/files/options_cancelack_2%highest_latencies_{date}.csv'
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
    grouped_data.to_csv(f'/home/elliott/development/files/options_cancelack_2%highest_latencies_{date}_summary.csv', index=False)
    
    #write_to_database(date)

if __name__ == '__main__':
    main()  