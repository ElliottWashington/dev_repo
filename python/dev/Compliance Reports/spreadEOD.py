import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import pandas as pd
import psycopg2
import smtplib
import pandas.io.sql as psql
import pandas as pd
import warnings
import argparse
warnings.filterwarnings("ignore")

def EOD(date):
    info = []
    conn = psycopg2.connect(
        host="10.7.8.59", 
        database="automation", 
        user="trader", 
        password="scalp123", 
        port=5433
        )
    cur = conn.cursor()
    #today = datetime.now()
    #tomorrow = datetime.now() - relativedelta(days = 1)
    #today = today.strftime('%Y-%m-%d')
    #tomorrow = tomorrow.strftime('%Y-%m-%d')
    
    #d = datetime.datetime.today().strftime('%Y-%m-%d')
    #print (today)
    #print(tomorrow)
    
    for i in range(5):
        
        #Good Round Turns
        if i == 0:
            cur.execute(
                f"""
                    SELECT b.oe, a.symid, a.* FROM
                    (SELECT * FROM spreadarb_filteredlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND updatetype = 'ADD') a
                    JOIN
                    (SELECT a.event as oe, b.event ce, a.logtime ot, b.logtime as ct, a.opportunityid, a.instanceid FROM
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND (event = 'FILLED' OR event = 'PARTIAL')
                    AND poseffect = 'OPEN') a
                    JOIN
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND event = 'FILLED'
                    AND poseffect = 'CLOSE') b
                    ON a.instanceid= b.instanceid
                    AND a.opportunityid = b.opportunityid
                    AND a.filledqty = b.filledqty
                    AND a.strategyrun = b.strategyrun
                    AND a.roundturn = b.roundturn
                    AND a.lastexecutionid = b.openingexecutionid) b
                    ON a.opportunityid = b.opportunityid
                    AND a.instanceid = b.instanceid
                """)
            good_round_turns_count = cur.rowcount
            # print(f'Good Round Turns: {cur.rowcount}')
            info.append(f'Good Round Turns: {cur.rowcount}')

        #PIPs
        if i == 1:
            cur.execute(
                f"""
                    SELECT a.symid, b.*, a.* FROM
                    (SELECT * FROM spreadarb_filteredlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND updatetype = 'ADD') a
                    JOIN
                    (SELECT a.* FROM
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND (event = 'FILLED' OR event = 'PARTIAL')
                    AND poseffect = 'OPEN'
                    and lastfillpip > 0) a
                    JOIN
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND (event = 'FILLED' OR event = 'PARTIAL')
                    AND poseffect = 'CLOSE') b
                    ON a.instanceid= b.instanceid
                    AND a.opportunityid = b.opportunityid
                    AND a.filledqty = b.filledqty
                    AND a.strategyrun = b.strategyrun
                    AND a.roundturn = b.roundturn
                    AND a.lastexecutionid = b.openingexecutionid) b
                    ON a.opportunityid = b.opportunityid
                    AND a.instanceid = b.instanceid
                """
            )
            query = f"""
                    select a.instrument, b.leantimestamp, a.logtime, a.leanexchangeseqid, a.exchange, a.opportunityid, a.instanceid, a.username, a.strategyname, a.*, b.* from
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    and event = 'CANCELLED'
                    and poseffect = 'CLOSE') a
                    LEFT JOIN
                    (select * from spreadarb_filteredlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND updatetype = 'REMOVE') b
                    ON
                    a.opportunityid = b.opportunityid
                    AND a.instanceid = b.instanceid
                    -- WHERE b.logtime ISNULL
                    ORDER BY a.id DESC;
                """
            df_missedlean = psql.read_sql(query, conn)
            missed_lean_count = len(df_missedlean.index)
            info.append(f'PIP % on Good Round Turns: {(cur.rowcount/ (good_round_turns_count + missed_lean_count)) * 100:.2f}%')

        #Average PIP & Weighted PIP
        if i == 2:
            query = f"""
                    SELECT SUM(b.lastfillpip)/COUNT(b.lastfillpip) as averagepip, SUM(b.lastfillpip*b.filledqty)/SUM(b.filledqty) as aveweightedpip FROM
                    (SELECT * FROM spreadarb_filteredlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND updatetype = 'ADD') a
                    JOIN
                    (SELECT a.* FROM
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND (event = 'FILLED' OR event = 'PARTIAL')
                    AND poseffect = 'OPEN'
                    and lastfillpip > 0) a
                    JOIN
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND (event = 'FILLED' OR event = 'PARTIAL')
                    AND poseffect = 'CLOSE') b
                    ON a.instanceid= b.instanceid
                    AND a.opportunityid = b.opportunityid
                    AND a.filledqty = b.filledqty
                    AND a.strategyrun = b.strategyrun
                    AND a.roundturn = b.roundturn
                    AND a.lastexecutionid = b.openingexecutionid) b
                    ON a.opportunityid = b.opportunityid
                    AND a.instanceid = b.instanceid
                    GROUP BY b.poseffect

                """
            cur.execute(query)
            
            # info.append(f'Average pip: {cur}')
            df_pip = psql.read_sql(query, conn)
            average_pip = df_pip.iloc[0]['averagepip']
            avg_weighted_pip = df_pip.iloc[0]['aveweightedpip']
            info.append(f'Average Pip: {average_pip:.4f}')
            info.append(f'Average Weighted Pip: {avg_weighted_pip:.4f}')

        #Missed Leans & No Removes
        if i == 3:
            query = f"""
                    select a.instrument, b.leantimestamp, a.logtime, a.leanexchangeseqid, a.exchange, a.opportunityid, a.instanceid, a.username, a.strategyname, a.*, b.* from
                    (select * from spreadarb_orderlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    and event = 'CANCELLED'
                    and poseffect = 'CLOSE') a
                    LEFT JOIN
                    (select * from spreadarb_filteredlean
                    WHERE logtime >= TIMESTAMP '{date}' 
                    AND logtime < TIMESTAMP '{date}' + INTERVAL '1 DAY'
                    AND updatetype = 'REMOVE') b
                    ON
                    a.opportunityid = b.opportunityid
                    AND a.instanceid = b.instanceid
                    -- WHERE b.logtime ISNULL
                    ORDER BY a.id DESC;
                """
            cur.execute(query)
            info.append(f'Total Missed leans: {cur.rowcount} or{(cur.rowcount/ (good_round_turns_count + cur.rowcount)) * 100: .2f}%')
            df = psql.read_sql(query, conn)
            df_nolean = df['leantimestamp']
            no_removes = df_nolean.iloc[:, 0].isna().sum()
            info.append(f'No Removes: {no_removes}')
    
    #Your code here to populate the info list
    first_df = pd.DataFrame(info, columns=['Output'])
    return first_df

    if __name__ == "__main__":
    # Parse command line arguments
        parser = argparse.ArgumentParser(description="Run queries for a given date")
        parser.add_argument("date", type=str, help="Date in YYYY-MM-DD format")
        args = parser.parse_args()

def query_totals_by_account_daily(date):
    # Connect to PostgreSQL database
    conn2 = psycopg2.connect(
        host="10.7.8.59", 
        database="prop_reports", 
        user="scalp", 
        password="QAtr@de442", 
        port=5433
        )

    # Define SQL query with user-defined date parameter
    query = f"SELECT data_date, account, gross, total_trade_fees \
             FROM totals_by_account_daily \
             WHERE (account LIKE '20P%' OR account LIKE '24P%' OR account LIKE '27P%') \
             AND data_date = '{date}';"

    # Execute SQL query and fetch results
    accounts_totoal_df = pd.read_sql_query(query, conn2)

    # Close database connection
    conn2.close()

    # Return resulting dataframe
    return accounts_totoal_df

import csv
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def convert_df_to_html_table(df):
    return df.to_html(index=False, border=0, justify='left', classes='table')

def sendEmail(names, files, subject, body):
  
  # sending emails
  fromaddr = "reports@scalptrade.com"
  toaddr = names

  msg = MIMEMultipart()
  msg['From'] = fromaddr
  msg['To'] = ", ".join(toaddr)
  msg['Subject'] = '{}'.format(subject)

  # Attach the body as HTML
  html_table = convert_df_to_html_table(body)
  body_content = MIMEText(html_table, 'html')
  msg.attach(body_content)

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

    df1 = EOD(args.date)
    df2 = query_totals_by_account_daily(args.date)
    combined_df = pd.concat([df1, df2], axis=1)

    combined_df = combined_df[["data_date", "account", "gross", "total_trade_fees", "Output"]]
    combined_df['net_pnl'] = combined_df['gross'] - combined_df['total_trade_fees']
    combined_df = combined_df[["data_date", "account", "gross", "total_trade_fees", "net_pnl", "Output"]]
    combined_df['net_pnl'] = pd.to_numeric(combined_df['net_pnl'], errors='coerce')
    total_net_pnl = combined_df['net_pnl'].sum()

    # Add a row with "Total" label and the sum of 'net_pnl' column
    total_row = pd.DataFrame({
        'data_date': 'Total',
        'account': '',
        'gross': '',
        'total_trade_fees': '',
        'net_pnl': total_net_pnl,
        'Output': ''
    }, index=[len(combined_df)])
    combined_df = pd.concat([combined_df, total_row], axis=0, ignore_index=True)

    combined_df.fillna('         ', inplace=True)
    combined_df.rename(columns={
        'data_date': 'Date',
        'account': 'Account',
        'gross': 'Gross PnL',
        'total_trade_fees': 'Total Fees',
        'Output': 'Good Round Turns, PIPS, & Missed Leans'
    }, inplace=True)
    combined_df.to_csv(f'/home/elliott/development/files/spreadEOD_{args.date}.csv', index=False)

    names2 = ["ewashington@scalptrade.com"]
    names = ["firm@scalptrade.com"]
    files = [f"/home/elliott/development/files/spreadEOD_{args.date}.csv"]
    subject = f"Spread Arb EOD {args.date} "
    subject2 = f"Spread Arb EOD {args.date} (Late)"
    sendEmail(names, files, subject, combined_df)
