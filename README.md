## Shopify Exporter


Small project that polls a google sheet, carries out some transformations locally and posts the result to an output tab.


- Google Sheet : StockTake. #add link.

- Output. 

- Missing SKUs



How does work?

* A stocktake raw sheet is provided that is unpivoted.

* it is then merged against an existing shopify stocksheet.

* any missing SKUs that are found in the stocksheet are then posted to a missing SKU sheet.
