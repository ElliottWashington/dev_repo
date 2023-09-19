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

def findEntryF(clientID, position):
    position -= 1
    while (position > -1):
        if clientID == fast.iat[position,21]: #and (fast.iat[position, 23] == 0):
            try:
                fndDt = datetime.datetime.strptime(fastTimes.iat[position], '%Y-%m-%d %H:%M:%S.%f').strftime('%H:%M:%S.%f')
            except ValueError:
                fndDt = datetime.datetime.strptime(fastTimes.iat[position], '%Y-%m-%d %H:%M:%S').strftime('%H:%M:%S.%f')
            return fndDt
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

d = datetime.datetime.combine(datetime.date.today(), datetime.time()) - datetime.timedelta(days=1)
foundP = False
ftpFileName = 'DailyActivityLog_AOS_TBPRO3_' + d.strftime('%Y%m%d') + ".csv"
positionsFileName = 'Scalp_Trade_EXT011_' + d.strftime('%Y%m%d') + ".csv"
mysqlCnx = mysql.connector.connect(user='reportuser', password='$Calp456', host='172.30.68.39', database='compliance_reports',auth_plugin='mysql_native_password')
mysqlCnx.autocommit = True
sqlCursor = mysqlCnx.cursor()
conn = psycopg2.connect(host="172.30.99.59", database="fixtransactions", user="scalp", password="QAtr@de442")

#endregion

#region Download Fast Data from Postgre database

postQuery = "(select * from transactions where tag39 IS NOT NULL AND date_trunc('day',tag52)='" + d.strftime('%Y-%m-%d')+"' and tag1 != '21P530324' order by sequencenumber)"
postCur = conn.cursor()
postFile = open('/tmp/report-files/fast.csv', 'wb')
postCur.copy_to(postFile,postQuery ,',', null="(null)")
postFile.close()

#endregion

#region Download Sterling Data from RB FTP Server
ftp = SmartFTP()
ftp.ssl_version = ssl.PROTOCOL_SSLv23
ftp.connect('ftp.tradingblock.com', 21, 100)
ftp.auth()
ftp.prot_p()
print(ftp.login(user='tbftpuser02', passwd='Tr@d1ngBfTp85296'))
ftp.prot_p()

print(ftp.getwelcome())
print(ftp.cwd('/'))
localFile = open('/tmp/report-files/change.csv', 'wb')
print(ftp.retrbinary('RETR ' + ftpFileName, localFile.write, 1024))
localFileP = open('/tmp/report-files/positions.csv', 'wb')
try:
    print(ftp.retrbinary('RETR ' + positionsFileName, localFileP.write, 1024))
    foundP = True
except:
    d2 = d - datetime.timedelta(days=1)
    positionsFileName = 'Scalp_Trade_EXT011_' + d2.strftime('%Y%m%d') + ".csv"
    try:
        print(ftp.retrbinary('RETR ' + positionsFileName, localFileP.write, 1024))
        foundP = True
    except:
        d3 = d2 - datetime.timedelta(days=1)
        positionsFileName = 'Scalp_Trade_EXT011_' + d3.strftime('%Y%m%d') + ".csv"
        try:
            print(ftp.retrbinary('RETR ' + positionsFileName, localFileP.write, 1024))
            foundP = True
        except:
            print("error")


ftp.quit()
localFile.close()
localFileP.close()

#endregion

##where report making used to be???

# region ReportMaker
reportName = "'/tmp/report-files/Sterling_Fast_Combined_" + d.strftime('%Y%m%d') + ".csv'"
finalFile = open('/tmp/report-files/Report_Maker/Lib/file_maker.txt', 'r')
finalCom = finalFile.read().format(reportName)
# loadCom = open('/tmp/report-files/Report_Maker/Lib/data_update.txt', 'r').read()
reportName = reportName[1:-1]



try:
    fromFile('/tmp/report-files/Report_Maker/Lib/drops.txt', sqlCursor)
except mysql.connector.Error as err:
    print("sql error: {}".format(err))
try:
    fromFile('/tmp/report-files/Report_Maker/Lib/finish.sql', sqlCursor)
    print("Creating File...")
    sqlCursor.execute(finalCom)
    print("Boom")
except mysql.connector.Error as err:
    print("sql error: {}".format(err))

#endregion

sterling = pd.read_csv('/tmp/report-files/change.csv', sep=',', header=0)
positions = pd.read_csv('/tmp/report-files/positions.csv', sep=';', header=None)
fast = pd.read_csv('/tmp/report-files/fast.csv', sep=',', header=None, lineterminator='\n', usecols=range(128), low_memory=False)

#region Spoofing/Layering

positionDict = {}
symbolsList = []
for x in range(len(positions)):
    sym = positions[7][x].replace(" ", "")
    trader = positions[4][x]
    positionDict.setdefault(trader[-4:], {}).setdefault(sym, positions[10][x])
    if sym not in symbolsList:
        symbolsList.append(sym)

sterlingTimes = sterling['Time']
sterLength = len(sterlingTimes)
spoofs = pd.DataFrame(np.zeros((3500, 16)), columns=["Date", "Time", "Time Entered", "Side", "Symbol",
                                                     "Position", "Quantity", "Maturity", "Strike Price",
                                                     "Order Price", "Open Close", "Order Type", "OrderID",
                                                     "Status", "Exchange", "Account"])
spIt = 0


start = datetime.datetime.now()
for i in range(1, sterLength - 1):
    if sterling['Exe Qty'][i] < 1:
        continue
    temp = i + 1
    if '#' in sterlingTimes[temp]:
        continue
    symbol = sterling['Symbol'][i]
    account = sterling['ID'][i]
    tradr = account[2:6]
    indx = symbol.find(' ')
    amt = sterling['Exe Qty'][i]
    spacedSymbol = symbol
    t1 = datetime.datetime.strptime(sterlingTimes[i], '%m/%d/%Y %H:%M:%S.%f')
    t0 = datetime.datetime.strptime(sterlingTimes[temp], '%m/%d/%Y %H:%M:%S.%f')
    t2 = t0
    if sterling['Side'][i] == 'S':
        side = 'S'
    else:
        side = 'B'
    if (len(symbol) > 6):
        spacedSymbol = str(symbol[:indx]) + "20" + str(symbol[indx + 1:indx + 7]) + str(sterling['Put/Call'][i]) + "{:.3f}".format(
            sterling['Strike Price'][i])
        curPosition = positionDict.setdefault(tradr, {}).setdefault(spacedSymbol, 0)
    else:
        curPosition = positionDict.setdefault(tradr, {}).setdefault(symbol, 0)
    # spoofString = t1.strftime('%m/%d/%Y %H:%M:%S.%f') + "\n" + sterling['Exch Cl Ord ID'][i] + "   " +  sterling['Log'][i] +  "    " + sterling['ID'][i]+ "\n"
    spoofFlag = False
    tempDict = {}
    while (t2 - t1) < datetime.timedelta(seconds=2):
        symbolT = sterling['Symbol'][temp]
        accountT = sterling['ID'][temp]
        tradrT = accountT[2:6]
        indxT = symbolT.find(' ')
        amtT = sterling['Exe Qty'][temp]
        spacedSymbolT = symbolT
        if sterling['Side'][temp] == 'S':
            sideT = 'S'
        else:
            sideT = 'B'
        if (len(symbolT) > 6):
            spacedSymbolT = str(symbolT[:indx]) + "20" + str(symbolT[indx + 1:indx + 7]) + str(sterling['Put/Call'][temp]) + "{:.3f}".format(sterling['Strike Price'][temp])
            tempos = positionDict.setdefault(tradrT, {}).setdefault(spacedSymbolT, 0)
            tempPosition = tempDict.setdefault(tradrT, {}).setdefault(spacedSymbolT, tempos)
        else:
            tempos = positionDict.setdefault(tradrT, {}).setdefault(spacedSymbolT, 0)
            tempPosition = tempDict.setdefault(tradrT, {}).setdefault(symbolT, tempos)
        '''
        if(temp -i) > 10:
            break
        '''
        if sterling['Order Price'][i] != sterling['Order Price'][temp] and sterling['Status'][temp] == "Canceled" and \
                sterling['Strike Price'][i] == sterling['Strike Price'][temp] and sterling['Symbol'][i] == \
                sterling['Symbol'][temp] and (sterling['Side'][i] != sterling['Side'][temp]) and (
                sterling['Put/Call'][i] == sterling['Put/Call'][
            temp]):  # and sterling['Order Price'][i]== sterling['Order Price'][temp]
            if "CANCELED" in sterling['Log'][temp]:  # ("CXL" in sterling['Log'][temp])  or
                # spoofString += sterling['Exch Cl Ord ID'][temp] + "   " + sterling['Log'][temp] +  "   +" + str(t2-t1) + "   " + sterling['ID'][temp] + "\n"
                if not spoofFlag:
                    spoofFlag = True
                    spoofs.loc[spIt, "Date"] = d.date()
                    spoofs.loc[spIt, "Time"] = t1.strftime('%H:%M:%S.%f')
                    spoofs.loc[spIt, "Time Entered"] = "N/A"
                    spoofs.loc[spIt, "Side"] = side
                    spoofs.loc[spIt, "Symbol"] = symbol
                    spoofs.loc[spIt, "Quantity"] = amt
                    spoofs.loc[spIt, "Maturity"] = sterling['Maturity'][i]
                    spoofs.loc[spIt, "Strike Price"] = sterling['Strike Price'][i]
                    spoofs.loc[spIt, "Order Price"] = sterling['Order Price'][i]
                    spoofs.loc[spIt, "Open Close"] = sterling['Open/Close'][i]
                    spoofs.loc[spIt, "Order Type"] = sterling['Instrument'][i]
                    spoofs.loc[spIt, "Status"] = sterling['Status'][i]
                    spoofs.loc[spIt, "OrderID"] = sterling['Exch Cl Ord ID'][i]
                    spoofs.loc[spIt, "Exchange"] = sterling['Destination'][i]
                    spoofs.loc[spIt, "Account"] = account
                    spoofs.loc[spIt, "Position"] = curPosition
                    iSpIt = spIt
                    spIt += 1
                    if (len(symbol) > 6):
                        tempDict.setdefault(tradr, {}).setdefault(spacedSymbol, 0)

                    else:
                        tempDict.setdefault(tradr, {}).setdefault(symbol, 0)
                    if side == 'S':
                        tempDict[tradr][spacedSymbol] -= amt
                    else:
                        tempDict[tradr][spacedSymbol] += amt

                symbol = sterling['Symbol'][temp]
                spoofs.loc[spIt, "Date"] = d.date()
                spoofs.loc[spIt, "Time"] = t2.strftime('%H:%M:%S.%f')
                spoofs.loc[spIt, "Time Entered"] = findEntry(sterling['Exch Cl Ord ID'][temp], temp)
                spoofs.loc[spIt, "Side"] = sterling['Side'][temp]
                spoofs.loc[spIt, "Symbol"] = symbol
                spoofs.loc[spIt, "Quantity"] = sterling['Order Quantity'][temp]
                spoofs.loc[spIt, "Maturity"] = sterling['Maturity'][temp]
                spoofs.loc[spIt, "Strike Price"] = sterling['Strike Price'][temp]
                spoofs.loc[spIt, "Order Price"] = sterling['Order Price'][temp]
                spoofs.loc[spIt, "Order Type"] = sterling['Instrument'][temp]
                spoofs.loc[spIt, "Status"] = sterling['Status'][temp]
                spoofs.loc[spIt, "OrderID"] = sterling['Exch Cl Ord ID'][temp]
                spoofs.loc[spIt, "Exchange"] = sterling['Destination'][temp]
                account = sterling['ID'][temp]
                spoofs.loc[spIt, "Account"] = account
                spoofs.loc[spIt, "Open Close"] = sterling['Open/Close'][temp]
                spoofs.loc[spIt, "Position"] = tempDict[tradrT][spacedSymbolT]

                spIt += 1
        temp += 1
        if temp == (sterLength - 1):
            break
        t2 = datetime.datetime.strptime(sterlingTimes[temp], '%m/%d/%Y %H:%M:%S.%f')

    if (side == 'S'):
        positionDict[tradr][spacedSymbol] -= amt
    else:
        positionDict[tradr][spacedSymbol] += amt

    if t0 - t1 < datetime.timedelta(seconds=2) and spoofFlag:
        spoofs.loc[spIt, "Date"] = ' '
        spoofs.loc[spIt, "Time"] = ' '
        spoofs.loc[spIt, "Time Entered"] = ' '
        spoofs.loc[spIt, "Side"] = ' '
        spoofs.loc[spIt, "Symbol"] = ' '
        spoofs.loc[spIt, "Quantity"] = ' '
        spoofs.loc[spIt, "Maturity"] = ' '
        spoofs.loc[spIt, "Strike Price"] = ' '
        spoofs.loc[spIt, "Order Price"] = ' '
        spoofs.loc[spIt, "Order Type"] = ' '
        spoofs.loc[spIt, "Status"] = ' '
        spoofs.loc[spIt, "OrderID"] = ' '
        spoofs.loc[spIt, "Exchange"] = ' '
        spoofs.loc[spIt, "Account"] = ' '
        spoofs.loc[spIt, "Open Close"] = ' '
        spoofs.loc[spIt, "Position"] = ' '

        spIt += 1
print(datetime.datetime.now() - start)
spoofs = spoofs[:spIt]
spoofFile = "/tmp/report-files/Spoofs_" + d.strftime('%Y%m%d') + ".csv"
spoofs.to_csv(spoofFile, date_format="hh:mm:ss.000")

#endregion

#region Spoofing & Layering Fast

positionDict = {}
symbolsList = []
for x in range(len(positions)):
    sym = positions[7][x].replace(" ", "")
    trader = positions[4][x]
    positionDict.setdefault(trader[-4:], {}).setdefault(sym, positions[10][x])
    if sym not in symbolsList:
        symbolsList.append(sym)

print(positionDict.keys())
fastLength = fast.shape[0]
fastTimes = fast.iloc[:, 31]
spoofs = pd.DataFrame(np.zeros((3500, 16)), columns=["Date", "Time", "Time Entered", "Side", "Symbol",
                                                     "Position", "Quantity", "Maturity", "Strike Price",
                                                     "Order Price", "Open Close", "Order Type", "OrderID",
                                                     "Status", "Exchange", "Account"])
spIt = 0
start = datetime.datetime.now()
for i in range(fastLength - 1):
    if fast.iat[i, 17] == '0' or fast.iat[i, 17] == '(null)':
        continue
    temp = i + 1
    while (fast.iat[temp, 22] == '(null)'):
        temp += 1
    symbol = fast.iat[i, 33]
    account = fast.iat[i, 1]
    tradr = account[3:7] if str(account[2]).isalpha() else account[2:6]
    amt = int(fast.iat[i, 17])
    spacedSymbol = symbol
    try:
        t1 = datetime.datetime.strptime(fastTimes.iat[i], '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        t1 = datetime.datetime.strptime(fastTimes.iat[i], '%Y-%m-%d %H:%M:%S')
    try:
        t0 = datetime.datetime.strptime(fastTimes.iat[temp], '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        t0 = datetime.datetime.strptime(fastTimes.iat[temp], '%Y-%m-%d %H:%M:%S')

    t2 = t0
    if fast.iat[i, 32] == '2':  # side 2 = sell?
        side = 'S'
    else:
        side = 'B'
    if fast.iat[i, 68] != '(null)':
        spacedSymbol = str(symbol) + str(fast.iat[i, 66]) + str(fast.iat[i, 71]) + (
            'C' if fast.iat[i, 67] == 0 else 'P') + "{:.3f}".format(float(fast.iat[i, 68]))
        curPosition = positionDict.setdefault(tradr, {}).setdefault(spacedSymbol, 0)
    else:
        curPosition = positionDict.setdefault(tradr, {}).setdefault(symbol, 0)
    # spoofString = t1.strftime('%m/%d/%Y %H:%M:%S.%f') + "\n" + fast[7][i] + "   " +  sterling['Log'][i] +  "    " + fast[1][i]+ "\n"
    spoofFlag = False
    tempDict = {}
    while (t2 - t1) < datetime.timedelta(seconds=2.0):
        symbolT = fast.iat[temp, 33]
        accountT = fast.iat[temp, 1]
        tradrT = accountT[3:7] if str(accountT[2]).isalpha() else accountT[2:6]
        indxT = symbolT.find(' ')
        amtT = int(fast.iat[temp, 22])
        spacedSymbolT = symbolT
        if fast.iat[temp, 32] == '2':
            sideT = 'S'
        else:
            sideT = 'B'
        if fast.iat[temp, 68] != '(null)':
            spacedSymbolT = str(symbol) + str(fast.iat[temp, 66]) + str(fast.iat[temp, 71]) + (
                'C' if fast.iat[temp, 67] == 0 else 'P') + "{:.3f}".format(float(fast.iat[temp, 68]))
            tempos = positionDict.setdefault(tradrT, {}).setdefault(spacedSymbolT, 0)
            tempPosition = tempDict.setdefault(tradrT, {}).setdefault(spacedSymbolT, tempos)
        else:
            tempos = positionDict.setdefault(tradrT, {}).setdefault(spacedSymbolT, 0)
            tempPosition = tempDict.setdefault(tradrT, {}).setdefault(symbolT, tempos)
        '''
        if(temp -i) > 10:
            break

        if(symbol == symbolT and side != sideT and fast.iat[temp,68] == fast.iat[i,68] and fast.iat[temp,27] != fast.iat[i,27] and fast.iat[temp,23] == 4 and fast.iat[temp,67] == fast.iat[i,67]):
            print("XD")
        '''
        if fast.iat[i, 27] != fast.iat[temp, 27] and fast.iat[temp, 23] == 4 and \
                fast.iat[i, 68] == fast.iat[temp, 68] and fast.iat[i, 33] == \
                fast.iat[temp, 33] and (side != sideT) and (
                fast.iat[i, 67] == fast.iat[temp, 67]) and (
                (str(fast.iat[i, 66]) + str(fast.iat[i, 71])) == (str(fast.iat[temp, 66]) + str(fast.iat[temp, 71]))):

            timeEntered = findEntryF(fast.iat[temp, 21], i)
            if not spoofFlag and timeEntered != "Not Found":
                spoofFlag = True
                spoofs.loc[spIt, "Date"] = d.date()
                spoofs.loc[spIt, "Time"] = t1.strftime('%H:%M:%S.%f')
                spoofs.loc[spIt, "Time Entered"] = "N/A"
                spoofs.loc[spIt, "Side"] = side
                spoofs.loc[spIt, "Symbol"] = spacedSymbol
                spoofs.loc[spIt, "Quantity"] = amt
                spoofs.loc[spIt, "Maturity"] = str(fast.iat[i, 66]) + str(fast.iat[i, 71])
                spoofs.loc[spIt, "Strike Price"] = fast.iat[i, 68]
                spoofs.loc[spIt, "Order Price"] = fast.iat[i, 27]
                spoofs.loc[spIt, "Open Close"] = fast.iat[i, 41]
                spoofs.loc[spIt, "Order Type"] = fast.iat[i, 24]
                spoofs.loc[spIt, "Status"] = 'Filled' if fast.iat[i, 23] == 2 else 'Canceled'
                spoofs.loc[spIt, "OrderID"] = fast.iat[i, 21]
                spoofs.loc[spIt, "Exchange"] = fast.iat[i, 15]
                spoofs.loc[spIt, "Account"] = account
                spoofs.loc[spIt, "Position"] = curPosition
                iSpIt = spIt
                spIt += 1
                if fast.iat[i, 68] != '(null)':
                    tempDict.setdefault(tradr, {}).setdefault(spacedSymbol, 0)
                else:
                    tempDict.setdefault(tradr, {}).setdefault(symbol, 0)
                if side == 'S':
                    tempDict[tradr][spacedSymbol] -= amt
                else:
                    tempDict[tradr][spacedSymbol] += amt

            if timeEntered != 'Not Found':
                spoofs.loc[spIt, "Date"] = d.date()
                spoofs.loc[spIt, "Time"] = t2.strftime('%H:%M:%S.%f')
                spoofs.loc[spIt, "Time Entered"] = timeEntered
                print(timeEntered)
                spoofs.loc[spIt, "Side"] = sideT
                spoofs.loc[spIt, "Symbol"] = spacedSymbolT
                spoofs.loc[spIt, "Quantity"] = fast.iat[temp, 22]
                spoofs.loc[spIt, "Maturity"] = str(fast.iat[i, 66]) + str(fast.iat[i, 71])
                spoofs.loc[spIt, "Strike Price"] = fast.iat[temp, 68]
                spoofs.loc[spIt, "Order Price"] = fast.iat[temp, 27]
                spoofs.loc[spIt, "Order Type"] = fast.iat[temp, 24]
                spoofs.loc[spIt, "Status"] = 'Filled' if fast.iat[temp, 23] == 2 else 'Canceled'
                spoofs.loc[spIt, "OrderID"] = fast.iat[temp, 21]
                spoofs.loc[spIt, "Exchange"] = 'N/A'
                account = fast.iat[temp, 1]
                spoofs.loc[spIt, "Account"] = accountT
                spoofs.loc[spIt, "Open Close"] = fast.iat[temp, 41]
                spoofs.loc[spIt, "Position"] = tempDict[tradrT][spacedSymbolT]

                spIt += 1
        temp += 1
        if temp == (fastLength):
            break
        while fast.iat[temp, 22] == '(null)':
            temp += 1
        try:
            t2 = datetime.datetime.strptime(fastTimes[temp], '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            t2 = datetime.datetime.strptime(fastTimes[temp], '%Y-%m-%d %H:%M:%S')

    if (side == 'S'):
        positionDict[tradr][spacedSymbol] -= amt
    else:
        positionDict[tradr][spacedSymbol] += amt

    if t0 - t1 < datetime.timedelta(seconds=2) and spoofFlag:
        spoofs.loc[spIt, "Date"] = ' '
        spoofs.loc[spIt, "Time"] = ' '
        spoofs.loc[spIt, "Time Entered"] = ' '
        spoofs.loc[spIt, "Side"] = ' '
        spoofs.loc[spIt, "Symbol"] = ' '
        spoofs.loc[spIt, "Quantity"] = ' '
        spoofs.loc[spIt, "Maturity"] = ' '
        spoofs.loc[spIt, "Strike Price"] = ' '
        spoofs.loc[spIt, "Order Price"] = ' '
        spoofs.loc[spIt, "Order Type"] = ' '
        spoofs.loc[spIt, "Status"] = ' '
        spoofs.loc[spIt, "OrderID"] = ' '
        spoofs.loc[spIt, "Exchange"] = ' '
        spoofs.loc[spIt, "Account"] = ' '
        spoofs.loc[spIt, "Open Close"] = ' '
        spoofs.loc[spIt, "Position"] = ' '

        spIt += 1
print(datetime.datetime.now() - start)
print(positionDict.keys())
spoofs = spoofs[:spIt]
spoofFile2 = "/tmp/report-files/Spoofs_Fast_" + d.strftime('%Y%m%d') + ".csv"
spoofs.to_csv(spoofFile2, date_format='%s')

#endregion

#region Analyze Last Minute of Trades
lastPrices = {}
positionDict = {}
for x in range(len(positions)):
    sym = positions[7][x].replace(" ", "")
    trader = positions[4][x]
    positionDict.setdefault(trader[-4:], {}).setdefault(sym, positions[10][x])
    lastPrices[sym] = (positions[9][x], d)

sames = pd.DataFrame(np.zeros((3500, 19)), columns=["Date", "Time", "Log", "Symbol", "Last Price","Price_Time",
                                                     "Position", "Exe Quantity", "Order Quantity","Side", "Maturity",
                                                     "Strike Price", "Order Price", "Open Close", "Order Type", "OrderID",
                                                     "Status", "Exchange", "Account"])
spIt = 0

endIndexes = []
endDates = []
lastMin = d + datetime.timedelta(hours=15, minutes=58, seconds=59)
end = d + datetime.timedelta(hours=16, minutes=15)
start = datetime.datetime.now()
destinations = {}

sterlingTimes = sterling['Time']
sterLength = len(sterlingTimes)

#region Update Positions and Find Last Minute Trades

for i in range(1, sterLength):
    if '#' in sterlingTimes[i]:
        continue
    symbol = sterling['Symbol'][i]

    t = datetime.datetime.strptime(sterling['Time'][i], '%m/%d/%Y %H:%M:%S.%f')
    if t < lastMin:
        lastDate = datetime.datetime.min
        if symbol in lastPrices.keys():
            oldPos, lastDate = lastPrices[symbol]
        if sterling['Exe Qty'][i] < 1:
            if (t > lastDate) and (t < lastMin - datetime.timedelta(minutes=5)):
                lastPrices[symbol] = (sterling["Order Price"][i], t)
            continue
        if (t > lastDate) :
            lastPrices[symbol] = (sterling["Exe Price"][i],t)
        symbol = sterling['Symbol'][i]
        account = sterling['ID'][i]
        tradr = account[2:6]
        indx = symbol.find(' ')
        amt = sterling['Exe Qty'][i]
        spacedSymbol = symbol
        if sterling['Side'][i] == 'S':
            side = 'S'
        elif sterling['Side'][i] == 'B':
            side = 'B'
        else:
            side = 'T'
        if len(symbol) > 6:
            spacedSymbol = str(symbol[:indx]) + "20" + str(symbol[indx + 1:indx + 7]) + str(sterling['Put/Call'][i]) + "{:.3f}".format(
                sterling['Strike Price'][i])
            curPosition = positionDict.setdefault(tradr, {}).setdefault(spacedSymbol, 0)
        else:
            curPosition = positionDict.setdefault(tradr, {}).setdefault(symbol, 0)
        if (side == 'S' or side=='T'):
            positionDict[tradr][spacedSymbol] -= amt
        elif side == 'B':
            positionDict[tradr][spacedSymbol] += amt
    if t > lastMin and t < end:
        #if sterling['Exe Qty'][i] < 1:
            #continue
        endDates.append(t)
        endIndexes.append(i)

#endregion

endTimes = []
endIndexes = np.array(endIndexes)
endDates = np.array(endDates)
endIndexes = endIndexes[np.argsort(endDates)]

for i in endIndexes:
    endx = i
    #if sterling['Exe Qty'][endx] < 1:
        #continue
    t = datetime.datetime.strptime(sterling['Time'][endx], '%m/%d/%Y %H:%M:%S.%f')
    if t > lastMin and t < end:
        endTimes.append(t)
        symbol = sterling['Symbol'][endx]
        account = sterling['ID'][endx]
        tradr = account[2:6]
        indx = symbol.find(' ')
        amt = sterling['Exe Qty'][endx]
        spacedSymbol = symbol
        yahooSymbol = symbol
        if sterling['Side'][endx] == 'S' or sterling['Side'][endx] == 'T':
            side = 'S'
        elif sterling['Side'][endx] == 'B':
            side = 'B'
        else:
            side = 'T'
        if (len(symbol) > 6):
            spacedSymbol = str(symbol[:indx]) + "20" + str(symbol[indx + 1:indx + 7]) + str(sterling['Put/Call'][endx]) + "{:.3f}".format(sterling['Strike Price'][endx])
            curPosition = positionDict.setdefault(tradr, {}).setdefault(spacedSymbol, 0)
            yahooSymbol = str(symbol[:indx]) + str(symbol[indx + 1:indx + 7]) + str(sterling['Put/Call'][endx]) + "{:>09}".format("{:.3f}".format(sterling['Strike Price'][endx]))
            yahooSymbol = yahooSymbol.replace(".","")
        else:
            curPosition = positionDict.setdefault(tradr, {}).setdefault(symbol, 0)
        if (side == 'B' and curPosition > 0) or (side == 'S' and curPosition < 0):
            symbol = sterling['Symbol'][endx]
            sames.loc[spIt, "Date"] = d.date()
            sames.loc[spIt, "Time"] =  t.strftime('%H:%M:%S.%f')
            sames.loc[spIt, "Log"] = sterling['Log'][endx]
            sames.loc[spIt, "Side"] = sterling['Side'][endx]
            sames.loc[spIt, "Symbol"] = symbol
            sames.loc[spIt, "Order Quantity"] = sterling['Order Quantity'][endx]
            sames.loc[spIt, "Exe Quantity"] = sterling['Exe Qty'][endx]
            sames.loc[spIt, "Maturity"] = sterling['Maturity'][endx]
            sames.loc[spIt, "Strike Price"] = sterling['Strike Price'][endx]
            sames.loc[spIt, "Order Price"] = sterling['Order Price'][endx]
            sames.loc[spIt, "Order Type"] = sterling['Instrument'][endx]
            sames.loc[spIt, "Status"] = sterling['Status'][endx]
            sames.loc[spIt, "OrderID"] = sterling['Exch Cl Ord ID'][endx]
            sames.loc[spIt, "Exchange"] = sterling['Destination'][endx]
            account = sterling['ID'][endx]
            sames.loc[spIt, "Account"] = account
            sames.loc[spIt, "Open Close"] = sterling['Open/Close'][endx]
            sames.loc[spIt, "Position"] = positionDict[tradr][spacedSymbol]
            try:
                lastTrade = lastPrices[symbol]
                sames.loc[spIt, "Last Price"] = lastTrade[0]
                sames.loc[spIt, "Price_Time"] = lastTrade[1].strftime('%H:%M:%S.%f')
            except:
                sames.loc[spIt, "Last Price"] = "NotFound"
                sames.loc[spIt, "Price_Time"] = "NotFound"
            '''
            try:
                data = yf.download(yahooSymbol, d.strftime('%Y-%m-%d'), period='1d')
                print(data)
                day = data.iloc[:,0]
                data = data['Close']
                sames.loc[spIt, "Closing Price"] = data[0]
                sames.loc[spIt, "LastClose"] = day[0]
            except:
                sames.loc[spIt, "Closing Price"] = "None"
                sames.loc[spIt, "LastClose"] = "None"
            '''
            spIt+=1
        ####update values
        if sterling['Exe Qty'][endx] < 1:
            #lastPrices[symbol] = (sterling['Order Price'][endx], t)
            continue
        if (side == 'S' or side=='T'):
            positionDict[tradr][spacedSymbol] -= amt
        elif side == 'B':
            positionDict[tradr][spacedSymbol] += amt
        #lastPrices[symbol] = (sterling['Exe Price'][endx],t)
print(datetime.datetime.now() - start)
sames = sames[:spIt]
sames['Time'] = sames.Time.astype('str')
sames['Price_Time'] = sames.Price_Time.astype('str')
#sames['LastClose'] = sames.LastClose.astype('str')
sameFile = "/tmp/report-files/sames_" + d.strftime('%Y%m%d') + ".xlsx"
sames.to_excel(sameFile)


#reportName = "'/tmp/report-files/Report_Maker/Sterling_Fast_Combined_" + d.strftime('%Y%m%d') + ".csv'"
#endregion

#region Get contracts executed per destination

total_contracts = 0
for i in range(1, sterLength):
    if sterling['Exe Qty'][i] > 0 and sterling['Strike Price'][i] > 0:
        contracts_executed = int(sterling['Exe Qty'][i])
        total_contracts += contracts_executed
        current_contracts = destinations.setdefault(sterling['Destination'][i], [0])[0]
        destinations[sterling['Destination'][i]][0] = current_contracts + contracts_executed

destinations['Total Contracts'] = total_contracts
per_destination = pd.DataFrame.from_dict(destinations)
destinationFile = '/tmp/report-files/ContractsExecutedPerDestination_' + d.strftime('%Y%m%d') + ".csv"
per_destination.to_csv(destinationFile, index=False)

#endregion

#region Send Email of Finished File

fromaddr = "interntylera@gmail.com"
toaddr = ['joel.zawko@scalptrade.com', 'niral.patel@rbtrader.com', 'bgioia@rbtrader.com']
#toaddr = ['niral.patel@rbtrader.com']

msg = MIMEMultipart()
msg['From'] = fromaddr
msg['To'] = ", ".join(toaddr)
msg['Subject'] = "Reports " + d.strftime('%m/%d/%Y')

attachment = open(reportName, "rb")
p = MIMEBase('application', 'octet-stream')
p.set_payload(attachment.read())
encoders.encode_base64(p)
p.add_header('Content-Disposition', "attachment; filename= %s" % reportName.split("/")[-1])
msg.attach(p)

attachment2 = open(spoofFile, "rb")
p2 = MIMEBase('application', 'octet-stream')
p2.set_payload(attachment2.read())
encoders.encode_base64(p2)
p2.add_header('Content-Disposition', "attachment; filename= %s" % spoofFile.split("/")[-1])
msg.attach(p2)

attachment3 = open(sameFile, "rb")
p3 = MIMEBase('application', 'octet-stream')
p3.set_payload(attachment3.read())
encoders.encode_base64(p3)
p3.add_header('Content-Disposition', "attachment; filename= %s" % sameFile.split("/")[-1])
msg.attach(p3)

attachment4 = open(destinationFile, "rb")
p4 = MIMEBase('application', 'octet-stream')
p4.set_payload(attachment4.read())
encoders.encode_base64(p4)
p4.add_header('Content-Disposition', "attachment; filename= %s" % destinationFile.split("/")[-1])
msg.attach(p4)

attachment5 = open(spoofFile2, "rb")
p5 = MIMEBase('application', 'octet-stream')
p5.set_payload(attachment5.read())
encoders.encode_base64(p5)
p5.add_header('Content-Disposition', "attachment; filename= %s" % spoofFile2.split("/")[-1])
msg.attach(p5)

s = smtplib.SMTP('smtp.gmail.com', 587)
s.starttls()
s.login(fromaddr, "cheesit97")
text = msg.as_string()

s.sendmail(fromaddr, toaddr, text)

s.quit()

#endregion
