#!/usr/local/bin/python3.7
#import mysql.connector
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import mysql.connector
import psycopg2
import ftplib
from ftplib import FTP_TLS
import ssl
import pandas as pd
import datetime
import numpy as np


def findEntry(clientID, position):
    position -= 1
    while (position > -1):
        if clientID == sterling['Exch Cl Ord ID'][position] and ("CXL" not in sterling['Log'][position]) and (
                "BUY" in sterling['Log'][position] or "SELL" in sterling['Log'][position]):
            return datetime.datetime.strptime(sterlingTimes[position], '%m/%d/%Y %H:%M:%S.%f').strftime('%H:%M:%S.%f')
        position -= 1
    return "Not Found"

class SmartFTP(FTP_TLS):
        def makepasv(self):
            invalidhost, port = super(SmartFTP, self).makepasv()
            return self.host, port

        def ntransfercmd(self, cmd, rest=None):
            conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
            if self._prot_p:
                conn = self.context.wrap_socket(conn,
                                                server_hostname=self.host,
                                                session=self.sock.session)  # this is the fix
            return conn, size

def fromFile(filename, cursor):
        # Open and read the file as a single buffer
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()

        # all SQL commands (split on ';')
        sqlCommands = sqlFile.split(';')
        for command in sqlCommands:
            print("Executing Command:")
            print(command)
            print("***************************************************************************************")
            print("\n\n")
            cursor.execute(command)

#region Make connections

d = datetime.datetime.today()
nowDog = datetime.datetime.today()
#d = datetime.datetime.combine(datetime.date.today(), datetime.time()) - datetime.timedelta(days=4)
#d = datetime.datetime.today().strftime('%Y%m%d')
print(d.strftime('%Y%m%d'))
foundP = False
ftpFileName = 'Scalp_Trade_EXT001_' + d.strftime('%Y%m%d') + ".csv"
#dailyActName = "DailyActivityLog_AOS_TBPRO3_" + d.strftime('%Y%m%d') + ".csv"
positionsFileName = 'Scalp_Trade_EXT011_' + d.strftime('%Y%m%d') + ".csv"
d = datetime.datetime.combine(datetime.date.today(), datetime.time()) - datetime.timedelta(days=1)
dailyAct = "DailyActivityLog_AOS_TBPRO3_" + d.strftime('%Y%m%d') + ".csv"
mysqlCnx = mysql.connector.connect(user='reportuser', password='$Calp456', host='172.30.68.39', database='compliance_reports',auth_plugin='mysql_native_password')
mysqlCnx.autocommit = True
sqlCursor = mysqlCnx.cursor()
#conn = psycopg2.connect(host="10.11.71.31", database="fixtransactions", user="scalp", password="QAtr@de442")

#endregion
filelist=[]
#region Download Sterling Data from RB FTP Server
ftp = SmartFTP()
ftp.ssl_version = ssl.PROTOCOL_SSLv23
ftp.connect('ftp.tradingblock.com', 21, 100)
ftp.auth()
ftp.prot_p()
print(ftp.login(user='tbftpuser02', passwd='Tr@d1ngBfTp85296'))
ftp.prot_p()

def downloadDaily(ftp, filename):
    locations = '/tmp/trading/assignmentFile/{}'.format(filename)
    ftp.retrbinary("RETR " + filename, open(locations,"wb").write, 1024)
def downloadPosition(ftp, filename):
    # Datetime
    d = datetime.datetime.combine(datetime.date.today(), datetime.time()) - datetime.timedelta(days=1)
    positionsFileName = 'Scalp_Trade_EXT590_' + d.strftime('%Y%m%d') + ".csv"

    locations = '/tmp/trading/assignmentFile/{}'.format(positionsFileName)
    print(locations)
    ftp.retrbinary("RETR " + filename, open(locations,"wb").write, 1024)
ftp.cwd("/")
ftp.dir('-t',filelist.append)
#ftp.retrlines('LIST',filelist.append)
countDaily = 0
countPosition = 0
for latest in filelist:
    #print(latest)
    #print(d.strftime('%Y%m%d'))
    '''
    if "Scalp_Trade_EXT590_" in latest and countDaily == 0:
        splitUp = latest.split()
        print(splitUp[8])
        countDaily = countDaily + 1
        downloadDaily(ftp, splitUp[8])
    '''
    if "Scalp_Trade_EXT590_" in latest and countPosition == 0:
        splitUp = latest.split()
        print(splitUp[8])
        countPosition = countPosition + 1 
        downloadPosition(ftp,splitUp[8])
ftp.quit()

