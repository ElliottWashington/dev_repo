import pandas as pd
import zipfile
import mysql.connector
import glob
import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

date = datetime.datetime.now().strftime('%Y%m%d')

def get_data(date):
    mydb = mysql.connector.connect(
        user='RBadmin',
        password='$Calp123',
        host='10.5.1.32',
        database='rbandits2',
        ssl_disabled=True
    )
    query = f"""
        SELECT ts as Time, sprdsym as Spread, price as Price
        FROM trdsprd
        WHERE ts >= DATE_SUB('{date}', INTERVAL 7 DAY)
        AND ts < '{date} 09:30:00'
        ORDER BY ts DESC
    """
    df = pd.read_sql_query(query, mydb)
    mydb.close()
    return df

def convert_to_new_format(option):
    if pd.isnull(option):
        return ''  # Return an empty string for NaN values

    parts = option.split('_')  # Split the spread into individual options
    new_parts = []
    for part in parts:
        # Identify the beginning of the date substring
        date_start = None
        for i in range(len(part) - 5):  # Subtract 5 to avoid running off the end of the string
            if part[i:i+2].isdigit() and part[i+2:i+4].isdigit() and part[i+4:i+6].isdigit():
                date_start = i
                break
        # If a date substring couldn't be found, treat the part as a non-option
        if date_start is None:
            new_parts.append(part)
            continue
        # Extract underlying, date, call/put, strike, and suffix
        underlying = part[:date_start]
        date = '20' + part[date_start:date_start+6]  # Convert YY to YYYY
        cp = part[date_start+6]
        strike_start = date_start + 7
        strike_end = strike_start
        for char in part[strike_start:]:
            if char.isdigit() or char == '.':
                strike_end += 1
            else:
                break
        strike = part[strike_start:strike_end]
        # Append decimal and trailing zeros if necessary
        if '.' not in strike:
            strike += '.00'
        elif len(strike.split('.')[1]) == 1:
            strike += '0'
        suffix = part[strike_end:]
        new_part = underlying + date + cp + strike + suffix
        new_parts.append(new_part)
    return '_'.join(new_parts)  # Join the options back into a spread

def scp_files(remote_path, local_path, hostname, username, password):
    # Build the SCP command
    cmd = f'sshpass -p "{password}" scp -o stricthostkeychecking=no {username}@{hostname}:{remote_path} {local_path}'

    # Execute the command
    os.system(cmd)

def send_email(names, subject, attachment_path):
    fromaddr = "reports@scalptrade.com"
    toaddr = names

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = ", ".join(toaddr)
    msg['Subject'] = '{}'.format(subject)

    # Attach the CSV file
    attachment = open(attachment_path, 'rb')
    part = MIMEBase('application', 'octet-stream')
    part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(attachment_path)}"')
    msg.attach(part)

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(fromaddr, "sc@lptrade")
    text = msg.as_string()
    s.sendmail(fromaddr, toaddr, text)

def run():
    # Now call the function with your parameters
    scp_files(
        remote_path=f'/volume1/Storage/snapshotdata/spreads/*{date}*.csv',
        local_path='.',
        hostname='10.7.8.20',
        username='admin',
        password='ScalpTrade2021!'
    )

    files = glob.glob(f'*{date}*.csv')

    df_list = []

    for file in files:
        if os.stat(file).st_size == 0:
            pass
        else:
            df = pd.read_csv(file)
            df_list.append(df)

    #sets date of current day to date variable.
    main_df = pd.concat(df_list)
    main_df = main_df[['Symbol', 'BBOAsk', 'BBOBid']]
    main_df = main_df[(main_df['BBOBid'] > 0) | (main_df['BBOAsk'] > 0)]
    main_df['formatted_symbol'] = main_df['Symbol'].apply(convert_to_new_format)
    db_df = get_data(date)
    merged_df = db_df.merge(main_df, left_on='Spread', right_on='formatted_symbol', how='right')
    merged_df.dropna(inplace=True)
    merged_df.drop(columns=['Symbol','Spread'], inplace=True)
    merged_df.to_csv(f"e.watchlist_{date}.csv", index=False)  # Specify index=False to exclude the index column

    # Compress the CSV file into a zip archive
    zip_filename = f"e.watchlist_{date}.zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(f"e.watchlist_{date}.csv", arcname=f"e.watchlist_{date}.csv")

    # Email sending logic
    send_email(['ewashington@scalptrade.com', 'sleland@scalptrade.com', 'aiacullo@scalptrade.com', 'jfeng@scalptrade.com' ], 'Open Rotation Watchlist', attachment_path=f"e.watchlist_{date}.zip")

if __name__ == "__main__":
    run()

