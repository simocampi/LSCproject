#!/bin/bash

#remove compiled files
find . -name "*.pyc" -type f -delete

#spawn ssh user24@192.168.20.157 "cd LSCproject/; rm -rf DataManipulation/ *.py* Utils/ wav_manipulation/"
expect -c 'spawn ssh user24@192.168.20.157 "cd LSCproject/; rm -rf DataManipulation/ *.py* Utils/ wav_manipulation/";
expect "assword:";
send "user24\r";
interact'

echo "Removed old files."
echo

#scp -r DataManipulation/ Main.py Utils/ wav_manipulation/ user24@192.168.20.157:LSCproject

expect -c 'spawn scp -r DataManipulation/ Main.py Utils/ wav_manipulation/ user24@192.168.20.157:LSCproject;
expect "assword:";
send "user24\r";
interact'
#expect -c 'scp -r DataManipulation/ Main.py Utils/ wav_manipulation/ user24@192.168.20.157:LSCproject;
#expect "assword:";
#send "user24\r";
#interact'