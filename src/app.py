from helpers.utils import ShopifyExport
from helpers import output_columns



def main():
    sf = ShopifyExport()
    sf.post_log("Started app")
    raw_df = sf.transform_raw_export('Wholesaler Data')
    missing_df = sf.create_shopify_export(raw_df=raw_df ,output_columns=output_columns)
    sf.service.clear_sheet(sheet='Output',rows=10000, cols=9)
    sf.service.df_to_sheet(sheet='Output', df=missing_df, index=False)
    

if __name__ == "__main__":
    main()
