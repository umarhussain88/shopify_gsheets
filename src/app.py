from helpers.utils import ShopifyExport, logger_util
from helpers import output_columns
from datetime import datetime
import gspread
from time import sleep 

    

logger = logger_util(__name__)





def main():
    
    logger.info('Starting app.')
    
    sf = ShopifyExport()
    gc = gspread.service_account(sf.src_path.joinpath('google_secret.json'))

    while True:
        sleep(30)
        logger.info('Running... every 30 seconds')
        val = gc.open('Shopify Export').get_worksheet_by_id(610921420).get('B2')[0][0]
        logger.info(f"{val} is the value in the spreadsheet")
        if val == 'TRUE':
            logger.info(f"{val} is the value in the spreadsheet running script")
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
            logger.info('Changing control value to false.')
            gc.open('Shopify Export').get_worksheet_by_id(610921420).update_acell('B2',False)
        else:
            sleep(30)

    

if __name__ == "__main__":
    main()
