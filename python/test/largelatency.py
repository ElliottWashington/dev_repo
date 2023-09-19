import pandas as pd
import psycopg2
import datetime
import argparse

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
    AND tag35 = 'AB';
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
    AND a.tag35 = 'AB'
    AND a.tag56 = '{target}';
    """

    # Execute the queries
    with conn.cursor() as cursor:
        cursor.execute(query1)
        cursor.execute(query2)

    # Load the query results into dataframes
    table2_df = pd.read_sql_query("SELECT * FROM temp_table2", conn)

    # Write dataframes to CSV files with date in the file names
    table2_df.to_csv(f"spread_nack_latencies_{date}.csv", index=False)
    
    # Close the database connection
    conn.close()

def write_csv(date, target):
    # Load the query results from CSV files
    table2_df = pd.read_csv(f"spread_nack_latencies_{date}.csv")
    
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
    result_df.to_csv(f"spread_cancelack_{target}_2%highest_latencies_{date}.csv", index=False, )

def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Analyze FIX latencies and create CSV reports.")
    parser.add_argument("date", help="Date to analyze, format: YYYY-MM-DD")
    parser.add_argument("target", help="Target entity code")
    parser.add_argument("sender", help="Sender entity code")
    args = parser.parse_args()

    # Validate the date format
    try:
        date = datetime.datetime.strptime(args.date, "%Y-%m-%d").date()
    except ValueError:
        print(f"Invalid date format. Please use 'YYYY-MM-DD'.")
        return

    # Run the queries and write the intermediate CSV
    run_queries(args.date, args.target, args.sender)

    # Write the final CSV file
    write_csv(args.date, args.target)

if __name__ == "__main__":
    main()