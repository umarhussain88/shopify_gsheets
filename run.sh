#!/bin/bash -l
cd /app/
#/usr/local/bin/pipenv run python3 src/app.py info
while true; do
        python3 src/app.py info
        echo "Sleeping for 2 minutes before the next run"
        sleep 120
done

