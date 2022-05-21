from helpers.utils import ShopifyExport
from helpers import output_columns
from datetime import datetime



def main():
    sf = ShopifyExport()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sf.post_log("Started app", start_time=now)
    raw_df = sf.transform_raw_export('Wholesaler Data')
    missing_df = sf.create_shopify_export(raw_df=raw_df ,output_columns=output_columns)
    sf.service.clear_sheet(sheet='Output',rows=10000, cols=9)
    sf.service.df_to_sheet(sheet='Output', df=missing_df[missing_df['source_data'].eq('both')].drop('source_data', axis=1), index=False)
    sf.service.clear_sheet(sheet='Missing SKUs',rows=3000, cols=3)
    sf.service.df_to_sheet(sheet='Missing SKUs',
                           df=missing_df[missing_df['source_data'].eq('missing')][['parent_sku','wholesaler_sku', '70 rue de la prulay']],
                           index=False)
    sf.post_log('Finished app', start_time=now)
    

if __name__ == "__main__":
    main()
