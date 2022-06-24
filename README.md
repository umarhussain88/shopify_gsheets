## Shopify Exporter


Small project that polls a google sheet, carries out some transformations locally and posts the result to an output tab.


- Google Sheet : StockTake. #add link.

- Output. 

- Missing SKUs



### How does work?

* A stocktake raw sheet is provided that is unpivoted.

* it is then merged against an existing shopify stocksheet.

* any missing SKUs that are found in the stocksheet are then posted to a missing SKU sheet.


### How to run?

On your google sheet click the Shopify Menu and Create Shopify Export.



### Requirements.

Two sheets are required.

- Wholesaler Data
- Shopify Lookup.

The default columns for Shopify Lookup are:
- Handle, Title, Option1 Name, Option1 Value and SKU

The default columns for the Wholesaler Data are

- STYLE NO, SIZE F and TOTAL 
- They must be in position 1,2 and Total will be in the final position.


### Deployment.

For deploying this app you will first need to clone the repository locally.

You will need a google service account  - details can be found below.

https://docs.gspread.org/en/latest/oauth2.html

Note, the service account should be called `google_secret.json` as per the gspread docs and be present in the same directory as the code.


### one time instructions

in a unix shell run the following commands

- `pip install --user pipenv`
- `cd ~/shopify_gsheets`
- `pipnev install Pipfile`
- `chmod +x .\run.sh`

open your crontab by using `crontab -e`

and add the following command

`*/2 * * * * /home/{your user}/shopify_gsheets/run.sh`

This will now periodically run every two minutes and run and pending jobs.
