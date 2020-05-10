#!/bin/bash

tar -cf database.tar ./Database
echo database archive created

expect -c 'spawn scp  database.tar user24@192.168.20.157:LSCproject;
expect "assword:";
send "user24\r";
interact'
echo uploaded database archive into the cluster

expect -c 'spawn ssh user24@192.168.20.157 "cd LSCproject/; rm -rf Database/; tar -xf database.tar;";
expect "assword:";
send "user24\r";
interact'
echo unzipped database archive

expect -c 'spawn ssh user24@192.168.20.157 "cd LSCproject/; hdfs dfs -rm -r user/user24/Database;echo removed database from hadoop filesystem; hdfs dfs -put Database user/user24";
expect "assword:";
send "user24\r";
interact'
echo uploaded new database files on hadoop filesystem
echo
echo removing database file in the cluster...

expect -c 'spawn ssh user24@192.168.20.157 "cd LSCproject/; rm -r database.tar Database";
expect "assword:";
send "user24\r";
interact'
echo
echo MISSION COMPLETE!
