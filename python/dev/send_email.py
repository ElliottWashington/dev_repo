import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import sys

def send_email(names, subject, body):
    fromaddr = "reports@scalptrade.com"
    toaddr = names

    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = ", ".join(toaddr)
    msg['Subject'] = '{}'.format(subject)
    msg.attach(MIMEText(body, 'plain'))

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.starttls()
    s.login(fromaddr, "sc@lptrade")
    text = msg.as_string()
    s.sendmail(fromaddr, toaddr, text)

def main():
    lines = []
    for line in sys.stdin:
        lines.append(line.strip())

    if lines:
        send_email(['ewashington@scalptrade.com'], 'Alert: sckt sess err detected', '\n'.join(lines))

if __name__ == "__main__":
    main()
