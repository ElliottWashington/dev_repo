#!/bin/bash

USERNAME="scalp"

PW=$(cat $HOME/.pw.txt)

SERVER=$1

sshpass -p $PW ssh $USERNAME@$SERVER


