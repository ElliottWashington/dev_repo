import pandas as pd
import argparse
from ftplib import FTP_TLS
import ftplib
import ssl

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

def download_from_ftp(ftp, filename, date):
    locations = f'/home/elliott/Development/scripts/python/dev/Penny Pilot/penny_pilot_{date}.txt'
    ftp.retrbinary("RETR " + filename, open(locations,"wb").write, 1024)

def get_file(date):  
    filelist=[]      
    ftp = SmartFTP()
    ftp.ssl_version = ssl.PROTOCOL_SSLv23
    ftp.connect('ftp.tradingblock.com', 21, 100)
    ftp.auth()
    ftp.prot_p()
    print(ftp.login(user='tbftpuser02', passwd='Tr@d1ngBfTp85296'))
    ftp.cwd("/")
    ftp.dir('-t',filelist.append)
    file = f'penny_pilot/penny_pilot_{date}.txt'
    download_from_ftp(ftp, file, date)     
    ftp.quit()       

def format_file(date):
    df = pd.read_csv(f'/home/elliott/Development/scripts/python/dev/Penny Pilot/penny_pilot_{date}.txt', delimiter='\t')  # assuming it's comma-separated
    # Filter the DataFrame
    filtered_df = df[df['Type'] != 'Non-Penny']

    # Extract 'Symbol' column and convert it into a list
    penny_list = filtered_df['Symbol'].tolist()
    df_penny = pd.DataFrame(penny_list, columns=['Symbol'])
    df_penny.to_csv("pennies.cfg", index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and process file for a given date.")
    parser.add_argument("date", type=str, help="Date in the format YYYY-MM-DD")
    args = parser.parse_args()

    get_file(args.date)
    format_file(args.date)