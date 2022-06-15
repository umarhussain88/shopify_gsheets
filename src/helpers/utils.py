from dataclasses import dataclass
from typing import Optional
import logging
from pathlib import Path
from gspread_pandas import Spread
from gspread_pandas.conf import get_config

import pandas as pd
import numpy as np

p = Path(__file__).parent.parent.joinpath("logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

if not Path(__file__).parent.parent.joinpath("logs").is_dir():
    Path(__file__).parent.parent.joinpath("logs").mkdir(parents=True)
    
    
p = Path(__file__).parent.parent.parent

c =  get_config(p)


@dataclass
class ShopifyExport:

    gsheet_log_sheet_name: str = "Logs"
    service: Spread = Spread("Shopify Export", config=c)

    # post init
    def __post_init__(self):
        object.__setattr__(self, "job_key", self.generate_job_key())

    def generate_job_key(self) -> int:

        df = self.service.sheet_to_df(sheet=self.gsheet_log_sheet_name, index=False)
        if df.shape[0] == 0:
            return 1
        else:
            return df["Job Key"].astype(int).iloc[-1] + 1

    def generate_time(self):
        return pd.Timestamp("now").strftime("%Y-%m-%d %H:%M:%S")

    def post_log(self, log_message: str, start_time: Optional[str] = None) -> None:

        if not start_time:
            start_time = self.generate_time()

        end = self.generate_time()

        # write to log sheet
        log_sheet = pd.DataFrame(
            {
                "Job Key": [self.job_key],
                "Start": [start_time],
                "End": [end],
                "Log": [log_message],
            }
        )

        # get max row
        max_row = self.service.sheet_to_df(sheet=self.gsheet_log_sheet_name).shape[0]

        self.service.df_to_sheet(
            df=log_sheet,
            sheet="Logs",
            headers=False,
            index=False,
            start=f"A{max_row + 2}",
        )

    def transform_raw_export(self, sheet_name: str) -> pd.DataFrame:
        """
        Transform raw export to a dataframe.
        """

        # open sheet
        df = self.service.sheet_to_df(sheet=sheet_name).reset_index().replace('', np.nan)
        # log start
        start_time = self.generate_time()
        self.post_log(f"Started transform_raw_export for {sheet_name}", start_time)

        df1 = df[df["SIZE F"].isnull()]


        accessories = (
            df.dropna(subset=["SIZE F"])
            .iloc[:, :2]
            .rename(columns={"STYLE NO": "SKU"})
            .melt(
                id_vars="SKU",
                var_name="Option1 Value",
                value_name="70 rue de la prulay",
            )
        )

        size_df = df1.iloc[:, :-1].drop("SIZE F", axis=1)

        size_df = size_df.rename(columns={size_df.columns[0]: "wholesaler_sku"})

        size_df_melted = pd.melt(
            size_df,
            id_vars="wholesaler_sku",
            var_name="Option1 Value",
            value_name="70 rue de la prulay",
        ).dropna(subset=["70 rue de la prulay"])

        size_df_melted["SKU"] = (
            size_df_melted["wholesaler_sku"].str.replace("\(.*\)", "", regex=True)
            + "-"
            + size_df_melted["Option1 Value"]
        ).str.strip()

        logger.info(f"shape of size_df_melted dataframe {size_df_melted.shape}")

        self.post_log(f"Finished transform_raw_export for {sheet_name}", start_time)

        final = pd.concat([size_df_melted.drop('wholesaler_sku',axis=1), accessories]).reset_index(drop=True)
        
        return final
    
    
    def create_parent_sku(self, dataframe : pd.DataFrame) -> pd.DataFrame:
        
        """creates a parent sku by stripping out anything in parenthesis 
           and removes the dash from the end of the sku
           
           in addition it upper cases the sku
           
        usage:
            df = create_parent_sku(df)
            
        example:
            'CA013-XS' -> 'CA013'
            'CA013(CLEANRANCE)' -> 'CA013'
            'ca013-xs' -> 'CA013'

        Returns:
            dataframe with parent sku column
        """
        dataframe['SKU'] = dataframe['SKU'].str.upper().str.strip()
        
        dataframe['parent_sku'] = dataframe['SKU'].str.replace("\(.*\)", "", regex=True)
        dataframe['parent_sku'] = dataframe['parent_sku'].str.split('-', expand=True)[0]
        
        return dataframe

    def create_shopify_export(
        self,
        raw_df: pd.DataFrame,
        output_columns: list,
    ) -> pd.DataFrame:

        # log start
        start_time = self.generate_time()
        self.post_log(f"Started create_shopify_export for output", start_time)

        dim_df = self.service.sheet_to_df(sheet="Shopify Lookup").reset_index()

        dim_df = dim_df.drop_duplicates(subset=["Handle", "Title", "Option1 Value"], keep="first")
        
        
        
        dim_df = self.create_parent_sku(dim_df)
        raw_df = self.create_parent_sku(raw_df)
        
        
        res1 = pd.merge(raw_df[raw_df['Option1 Value'].eq('SIZE F')].drop(["Option1 Value"], axis=1).rename(columns={'SKU' : 'wholesaler_sku'}),
                        dim_df,
                        how="left",
                        on="parent_sku",
                        indicator=True).rename(columns={'_merge' : 'source_data'})
        
        
        res2 = pd.merge(raw_df[raw_df['Option1 Value'].ne('SIZE F')].drop(["Option1 Value"],axis=1),
                        dim_df, how="left",
                        on=["SKU",'parent_sku'],
                        indicator=True).rename(columns={'_merge' : 'source_data'})
         
        
        
        # result = pd.merge(
        #     raw_df.drop(["Option1 Value"], axis=1).rename(columns={'SKU' : 'wholesaler_sku'}), dim_df, on=['parent_sku'],
        #     how="left",indicator=True
        # ).rename(columns={'_merge' : 'source_data'})

        result = pd.concat([res1,res2]).reset_index(drop=True)
        
        result['source_data'] = np.where(result['source_data'] == 'both', 'both', 'missing')
        
        output_columns.append('source_data')
        
        result_final = result.assign(
            **{col: pd.NA for col in output_columns if not col in result.columns}
        )[output_columns].sort_values(["SKU"])
        
        

        return result_final

    def create_missing_output(self, sheet_name: str) -> pd.DataFrame:
        pass
