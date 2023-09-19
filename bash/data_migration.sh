#!/bin/bash

mysqldump -u RBadmin -p -h 10.5.1.32 -P 2206 rbandits2 rbandits_dump.sql
