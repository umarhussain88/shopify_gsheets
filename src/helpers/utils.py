from dataclasses import dataclass
from typing import Optional
import logging
from pathlib import Path
from gspread_pandas import Spread

import pandas as pd


p = Path(__file__).parent.parent.joinpath("logs")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s:%(name)s:%(message)s")

if not Path(__file__).parent.parent.joinpath("logs").is_dir():
    Path(__file__).parent.parent.joinpath("logs").mkdir(parents=True)


@dataclass
class ShopifyExport:

    gsheet_log_sheet_name: str = "Logs"
    service: Spread = Spread("Shopify Export")

    # post init
    def __post_init__(self):
        object.__setattr__(self,'job_key', self.generate_job_key())



    def generate_job_key(self) -> int:

        df = self.service.sheet_to_df(sheet=self.gsheet_log_sheet_name,index=False)
        if df.shape[0] == 0:
            return 1 
        else:
          return df['Job Key'].astype(int).iloc[-1] + 1

    def generate_time(self):
        return pd.Timestamp("now").strftime("%Y-%m-%d %H:%M:%S")

    def post_log(self, log_message: str, start_time: Optional[str] = None) -> None:

        if not start_time:
            start_time = self.generate_time()

        end = self.generate_time()

        # write to log sheet
        log_sheet = pd.DataFrame({'Job Key': [self.job_key],'Start' : [start_time], 'End' : [end], 'Log': [log_message]})

        #get max row
        max_row = self.service.sheet_to_df(sheet=self.gsheet_log_sheet_name).shape[0]

        self.service.df_to_sheet(df=log_sheet,sheet='Logs',headers=False,index=False, start=f'A{max_row + 2}' )

    def transform_raw_export(self, sheet_name: str) -> pd.DataFrame:
        """
        Transform raw export to a dataframe.
        """

        # open sheet
        df = self.service.sheet_to_df(sheet=sheet_name)
        # log start
        start_time = self.generate_time()
        self.post_log(f"Started transform_raw_export for {sheet_name}", start_time)

        size_df = df.iloc[:, :-1].reset_index()

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

        # size_df_melted["SKU"] = size_df_melted["SKU"].str.replace("\sF", "", regex=True)

        logger.info(f"shape of size_df_melted dataframe {size_df_melted.shape}")

        # log end
        self.post_log(
            f"Finished transform_raw_export for {sheet_name}", start_time
        )

        return size_df_melted

    def create_shopify_export(
        self,
        raw_df: pd.DataFrame,
        output_columns: list,
    ) -> pd.DataFrame:

        # log start
        start_time = self.generate_time()
        self.post_log(f"Started create_shopify_export for output", start_time)

        dim_df = self.service.sheet_to_df(sheet="Shopify Lookup")

        result = pd.merge(
            raw_df.drop("Option1 Value", axis=1), dim_df, on=["SKU"], how="left"
        )

        # assign missing columns
        result = result.assign(
            **{col: pd.NA for col in output_columns if not col in result.columns}
        )
        result_final = result[output_columns]
        result_final = result_final.sort_values(["SKU"])

        return result_final[result_final["Handle"].isna() == True]

    def create_missing_output(self, sheet_name: str) -> pd.DataFrame:
        pass
