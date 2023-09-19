import argparse
import psycopg2
import csv

def write_to_database(date):
    # Construct the filename of the CSV file to read from
    filename = f"average_equity_nack_latencies_{date}.csv"

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
                "INSERT INTO average_latency_equity (date, exchange, count, average_difference) VALUES (%s, %s, %s, %s)",
                (date, exchange, counts, average_time_difference)
            )

            conn.commit()

    # Close the database connection
    conn.close()

# Parse the command-line arguments
parser = argparse.ArgumentParser(description="Add latency data to the database")
parser.add_argument("date", help="the date to use for the latency data (format: yyyy-mm-dd)")
args = parser.parse_args()

# Call the write_to_database function with the specified date
write_to_database(args.date)
