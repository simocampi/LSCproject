#!/bin/bash

ssh user24@192.168.20.157 "cd LSCproject/; rm -rf DataManipulation/ *.py* Utils/ wav_manipulation/"
echo "Removed old files."
echo

scp -r DataManipulation/ Main.py Utils/ wav_manipulation/ user24@192.168.20.157:LSCproject && echo "Uploaded all new files"
