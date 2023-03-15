#!/bin/bash

# Read all lines from a file
#conda activate uloztodownloader
FILE=$1
while read -r line
do
    python3 ulozto-downloader.py --parts 10 --auto-captcha $line
done < "$FILE"

echo "All done..."