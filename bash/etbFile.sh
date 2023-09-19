#if [ $DOW -eq 1 ]
#then
#  predays=$(date -d '-3 day' '+%Y%m%d')
#fi

DB_USER="RBadmin"
DB_PASSWD="\$Calp123"
TABLE="FTPFilesMonitoring"
DB_NAME="ultra"
server="172.30.68.39"
days=$(date -d '-1 day' '+%Y%m%d')

positions=$(find . -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -f2- -d" ")
tru="/tmp/trading/positions/"
truPositions="${tru}${positions}"
FILE="/tmp/trading/positions/Scalp_Trade_EXT011_${days}.csv"
dailyActivity=$(find . -type f -printf '%T@ %p\n' | sort -n | tail -1 | cut -f2- -d" ")
FILES="/tmp/trading/dailyactivity/DailyActivityLog_AOS_TBPRO3_${predays}.csv"
etb="/tmp/trading/etb.txt"
TACOBELL="/tmp/tb.csv"
FILES="/tmp/trading/dailyactivity/DailyActivityLog_AOS_TBPRO3_${predays}.csv"

cd /tmp/trading/positions/
cp $truePositions $FILE
cd /tmp/trading/dailyactivity/

for files in "$FILE" "$etb" "$dailyActivity" "$TACOBELL"; do
  b=$(basename $files)
  if [[ -s "$files" ]]
  then
    mysql -h "$server" --user="$DB_USER" --password="$DB_PASSWD" "$DB_NAME" << EOF
      INSERT INTO $TABLE (\`filename\`, \`server\`, \`success\`, \`filelocation\`) VALUES ("$b", "172.30.68.39", 1, "$files");
EOF
  else
    mysql -h "$server" --user="$DB_USER" --password="$DB_PASSWD" "$DB_NAME" << EOF
      INSERT INTO $TABLE (\`filename\`, \`server\`, \`success\`, \`filelocation\`) VALUES ("$b", "172.30.68.39", 0, "$files");
EOF
    /usr/bin/sendemail -t "niral.patel@rbtrader.com" -f "reports@rbtrader.com" -m "$files is empty for Lime" -u "$files is empty for Lime" -s "smtp.gmail.com:587" -o tls=yes -xu "reports@rbtrader.com" -xp "sc@lptrade"
  fi
done


scp /tmp/trading/etb.txt scalp@10.7.8.51:/home/scalp/or/etb.txt
scp /tmp/trading/etb.txt scalp@10.7.8.53:/home/scalp/or/etb.txt
scp /tmp/trading/etb.txt scalp@10.7.8.53:/home/scalp/or2/etb.txt
scp /tmp/trading/etb.txt scalp@10.7.8.55:/home/scalp/or/etb.txt
scp $FILE scalp@10.7.8.51:/home/scalp/SODFiles
scp $dailyActivity scalp@10.7.8.51:/home/scalp/SODFiles
