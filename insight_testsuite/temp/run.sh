#!/bin/bash

echo "Start unit test..."
if ! python3 ./src/find_political_donors_unittest.py ; then
    echo "An error occurred, exit."
    exit
fi


echo "Start process data..."
python3 ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt

