import pandas as pd
import psycopg2
import datetime
import argparse
import sys

def run_queries(date, target, sender):    
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        host="10.7.8.59",
        database="fixtransactions",
        user="scalp",
        password="QAtr@de442",
        port="5433"
    )
    
    # Define the SQL queries
    query1 = f"""
    CREATE TEMP TABLE temp_table1 AS
    SELECT tag11, tag100, tag41, tag49 
    FROM fixmsg 
    WHERE tag52 >= TIMESTAMP '{date}'
    AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND tag56 = '{target}'
    and tag49 = '{sender}'
    AND (tag35 = 'AB' or tag35 = 'D');
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


    with conn.cursor() as cursor:
        cursor.execute(query1)
        cursor.execute(query2)

 # Load the query results into dataframes
    table2_df = pd.read_sql_query("SELECT * FROM temp_table2", conn)

    # Write dataframes to CSV files with date in the file names
    table2_df.to_csv(f"/home/elliott/development/files/equity_nack_latencies_{date}.csv", index=False)
    
    # Close the database connection
    conn.close()

    return table2_df

def write_csv(date, target):
    # Load the query results from CSV files
    table2_df = pd.read_csv(f"/home/elliott/development/files/equity_nack_latencies_{date}.csv")
    
    # Calculate time differences and filter the data
    table2_df["cancel_52"] = pd.to_datetime(table2_df["cancel_52"])
    table2_df["cancelack_52"] = pd.to_datetime(table2_df["cancelack_52"])
    table2_df["time_diff"] = (table2_df["cancelack_52"] - table2_df["cancel_52"]).apply(lambda x: x.total_seconds())
    table2_df = table2_df.sort_values(by="time_diff", ascending=False)
    table2_df_filtered = table2_df[table2_df["time_diff"] >= 1]
    
    # Compute the threshold and filter the data again
    threshold = table2_df_filtered["time_diff"].quantile(0.98)
    top_2_percent = table2_df_filtered[table2_df_filtered["time_diff"] >= threshold]
    
    # Compute the counts and merge the dataframes
    counts = top_2_percent.groupby("tag100").size().reset_index(name="counts")
    result_df = pd.merge(top_2_percent, counts, on="tag100", how="left")
    
    # Write the final dataframe to a CSV file
    result_df.to_csv(f"/home/elliott/development/files/equity_cancelack_{target}_2%highest_latencies_{date}.csv", index=False, )

def run_analysis(date, target):
    # Read a CSV file located at '/home/elliott/development/files/options_nack_latencies_2023-03-10.csv' and store it in a variable called 'table2_df'
    table2_df = pd.read_csv(f'/home/elliott/development/files/equity_nack_latencies_{date}.csv')

    # Convert the 'cancel_52' column in 'table2_df' to datetime format using the 'pd.to_datetime()' function
    table2_df['cancel_52'] = pd.to_datetime(table2_df['cancel_52'])

    # Convert the 'cancelack_52' column in 'table2_df' to datetime format using the 'pd.to_datetime()' function
    table2_df['cancelack_52'] = pd.to_datetime(table2_df['cancelack_52'])

    # Calculate the time difference in seconds between 'cancel_52' and 'cancelack_52' columns in 'table2_df' and store it in a new column called 'time_diff'
    table2_df["time_diff"] = (table2_df["cancelack_52"] - table2_df["cancel_52"]).apply(lambda x: x.total_seconds())

    # Sort 'table2_df' in descending order based on 'time_diff'
    table2_df = table2_df.sort_values(by='time_diff', ascending=False)

    # Create a new DataFrame called 'table2_df_filtered' by filtering out rows where 'time_diff' equals 0 in 'table2_df'
    table2_df_filtered = table2_df[table2_df['time_diff'] != 0]

    # Calculate the 90th percentile of 'timeentile of 'time_diff_diffiff' in 'table2_df_filtered' and store it in a variable called 'threshold'
    threshold = table2_df_filtered['time_diff'].quantile(0.90)

    # Create a new DataFrame called 'top_2_percent' by filtering out rows in 'table2_df_filtered' where 'time_diff' is less than 'threshold'
    top_2_percent = table2_df_filtered[table2_df_filtered['time_diff'] >= threshold]

    # Count the occurrences of each value in the 'tag100' column in 'top_2_percent' and store the counts in a Series called 'counts'
    counts = top_2_percent.groupby('tag100')['tag100'].value_counts()

    # Drop the duplicate 'tag100' index from the 'counts' Series and store the result in a new DataFrame called 'single_df'
    single_df = counts.reset_index(level=1, drop=True)
    single_df = single_df.to_frame('counts')
    single_df = single_df.reset_index()

    # Merge 'single_df' into 'top_2_percent' based on the 'tag100' column and store the result in a new DataFrame called 'result_df'
    result_df = top_2_percent.merge(single_df, on='tag100', how='left')

    result_df.to_csv(f"/home/elliott/development/files/equity_cancelack_{target}_2%highest_latencies_{date}.csv", index=False)

def main():
    # Get command line arguments
    if len(sys.argv) != 4:
        print("Usage: python script.py <date> <target> <sender>")
        sys.exit(1)
    date = sys.argv[1]
    target = sys.argv[2]
    sender = sys.argv[3]

    # Call the 'run_queries' function with the command line arguments
    run_queries(date, target, sender)
    run_analysis(date, target)

if __name__ == '__main__':
    main()