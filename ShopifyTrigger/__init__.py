import logging

import azure.functions as func
from src import ShopifyExport, output_columns
from datetime import datetime





sf = ShopifyExport()




def run_shopify():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sf.post_log("Started app", start_time=now)
    raw_df = sf.transform_raw_export('Wholesaler Data')
    missing_df = sf.create_shopify_export(raw_df=raw_df ,output_columns=output_columns)

    missing_df['wholesaler_sku'] = missing_df['wholesaler_sku'].fillna(missing_df['SKU'])

    sf.service.clear_sheet(sheet='Output',rows=10000, cols=9)
    sf.service.df_to_sheet(sheet='Output', df=missing_df[missing_df['source_data'].eq('both')].drop('source_data', axis=1), index=False)
    sf.service.clear_sheet(sheet='Missing SKUs',rows=3000, cols=3)
    sf.service.df_to_sheet(sheet='Missing SKUs',
                           df=missing_df[missing_df['source_data'].eq('missing')][['parent_sku','wholesaler_sku', '70 rue de la prulay']],
                           index=False)
    sf.post_log('Finished app', start_time=now)
    


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        sf.post_log(f'HTTP Function Trigger: {name}', start_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))        
        run_shopify()
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
