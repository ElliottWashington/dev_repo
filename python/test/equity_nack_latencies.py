import argparse
import psycopg2
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
import os

def run_queries(date):
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
    SELECT 
    b.tag52 AS order_sent_time,
    a.tag52 AS ack_received_time,
    a.tag52 - b.tag52 AS time_difference,
    b.tag11 AS order_id,
    b.tag100 AS exchange
    FROM 
    fixmsg b 
    JOIN fixmsg a ON b.tag11 = a.tag11 
        AND b.tag35 = 'AB' 
        AND a.tag35 = '8'
    WHERE a.tag52 >= TIMESTAMP '{date}'
    AND a.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND b.tag52 >= TIMESTAMP '{date}'
    AND b.tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND b.tag56 = 'INCAPNS';
    """
    query2 = f"""
    create temp table temp_table2 as
    select time_difference, order_id, exchange from temp_table1;
    select * from temp_table2;
    """

    query3 = f"""
    CREATE TEMP TABLE temp_table3 AS
    SELECT 
        exchange, AVG(time_difference) as average_time_difference, COUNT(time_difference) AS counts
    FROM 
        temp_table2 
    GROUP BY 
        exchange;
    """
    # Execute the queries
    with conn.cursor() as cursor:
        cursor.execute(query1)
        cursor.execute(query2)
        cursor.execute(query3)


    # Load the query results into dataframes
    table2_df = pd.read_sql_query("SELECT * FROM temp_table2", conn)
    table3_df = pd.read_sql_query("SELECT * FROM temp_table3", conn)

    # Write dataframes to CSV files with date in the file names
    table2_df.to_csv(f"equity_nack_latencies_{date}.csv", index=False)
    table3_df.to_csv(f"average_equity_nack_latencies_{date}.csv", index=False)

    # Close the database connection
    conn.close()

def sendEmail(names, files, subject):

  # sending emails
  fromaddr = "reports@scalptrade.com"
  toaddr = names

  msg = MIMEMultipart()
  msg['From'] = fromaddr
  msg['To'] = ", ".join(toaddr)
  msg['Subject'] = '{}'.format(subject)

  # Attach file
  for x in files:
    attachment = open(x, "rb")
    p = MIMEBase('application', 'octet-stream')
    p.set_payload(attachment.read())
    encoders.encode_base64(p)
    p.add_header('Content-Disposition', "attachment; filename= %s" % "{}".format(x))
    msg.attach(p)

  # Sending email
  s = smtplib.SMTP('smtp.gmail.com', 587)
  s.starttls()
  s.login(fromaddr, "sc@lptrade")
  text = msg.as_string()

  s.sendmail(fromaddr, toaddr, text)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run queries for a given date")
    parser.add_argument("date", type=str, help="Date in YYYY-MM-DD format")
    args = parser.parse_args()

    run_queries(args.date)

    names = ["ewashington@scalptrade.com"]
    files = [f"average_equity_nack_latencies_{args.date}.csv"]
    subject = "Daily equity latency Report"

    sendEmail(names, files, subject)