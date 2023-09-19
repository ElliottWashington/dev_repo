import psycopg2
import csv
import smtplib
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import argparse

def identify_accounts_with_large_orders(date):
    db_credentials = {
        'dbname': 'fixtransactions',
        'user': 'scalp',
        'password': 'QAtr@de442',
        'host': '10.7.8.59',
        'port': '5433'
    }
    
    # Connect to the database using the credentials
    connection = psycopg2.connect(**db_credentials)
    
    # Create a cursor
    cursor = connection.cursor()

    query = f"""
    SELECT tag1 as account, tag167 as order_type, tag38 as quantity, tag11 as order_id, tag52 as date 
    FROM fixmsg
    WHERE tag52 >= TIMESTAMP '{date}'
    AND tag52 < TIMESTAMP '{date}' + INTERVAL '1 DAY'
    AND (
        (tag167 = 'OPT' AND tag38 > 75) OR
        (tag167 != 'OPT' AND tag38 > 10000)
    )
    ORDER BY tag52;
    """
    cursor.execute(query)
    
    # Fetch the results
    results = cursor.fetchall()
    
    # Close the cursor and connection
    cursor.close()
    connection.close()
    
    return results

def write_to_csv(results, date):
    with open(f'large_orders_{date}.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['account', 'order_type', 'quantity', 'order_id', 'date'])
        csv_writer.writerows(results)

def sendEmail(names, filename, subject):
  
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

def main():
    data = identify_accounts_with_large_orders(args.date)
    write_to_csv(data, args.date)
    print(f'Done!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run queries and analysis large orders.')
    parser.add_argument('date', help='Date for the queries (YYYY-MM-DD format)')
    args = parser.parse_args()
    main()  
    names2 = ["ewashington@scalptrade.com"]
    names = ["firm@scalptrade.com"]
    files = [f'large_orders_{args.date}.csv']
    subject = f"Daily Large Orders Report {args.date} "
    
    sendEmail(names2, files, subject)
    print(f"Email Sent to {names2}")
