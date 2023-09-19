#!/bin/bash

if [ $# -ne 2 ]; then
  echo "Usage: $0 <csv filename> <number of processes>"
  exit 2
fi

if [ ! -f "$1" ]; then
echo "Error: Argument 1 must be a file"
exit 2
fi

re='^[0-9]+$'
if ! [[ "$2" =~ $re ]] ; then
   echo "Error: Argument 1 must be a number"
   exit 2
fi

mypid=$BASHPID

tmpout="${mypid}.weights"

mkdir -p /tmp/binpack/$mypid

filename=$(basename $1)

directory=$(realpath $1 | rev | cut -d/ -f2- | rev)

no_ext=$(echo "$filename" | rev | cut -d. -f2- | rev)

ext=$(echo "$filename" | rev | cut -d. -f1 | rev)

pattern="${no_ext}_*${ext}"

cd /tmp/binpack/$mypid

cp "$directory/$filename" .

binpacking -f "$filename" -c packets_received -N "$2" -H >$tmpout 2>&1

if [ $? -ne 0 ]; then
cat $tmpout
cd ..
rm -rf /tmp/binpack/$mypid
exit 1
fi

echo process,line
tail -n +2 $(find -name "$pattern" | xargs) | \
 sed "s/[=][=][>] \(.*\)_\(.*\)[.]\(.*\) [<][=][=]/+\2/g" | \
 cut -d, -f1 | \
 sed '/^$/d' | \
 tr -s '\n' ',' | \
 tr -s '+' '\n' | \
 tail -n +2 | \
 sed 's/,*$//g'
echo

cd ..
rm -rf /tmp/binpack/$mypid

