import os
import argparse
import polars as pl
from datetime import datetime
import pytz
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

class Filter:
    def __init__(self, peer_review_dir, non_peer_review_dir, output_path, column_to_join="cited_doi"):
        self.peer_review_dir = peer_review_dir
        self.non_peer_review_dir = non_peer_review_dir
        self.output_path = output_path
        self.column_to_join = column_to_join


    def read_and_concatenate_dataframes(self, directory):
        dataframes = []
        for file in sorted(os.listdir(directory)):
            if file.endswith('.csv'):
                try:
                    df = pl.scan_csv(os.path.join(directory, file))
                    df_normalized = df.with_columns(cited_doi=pl.col("cited_doi").str.strip_chars("\n.").str.to_lowercase())
                    dataframes.append(df_normalized)
                except Exception as e:
                    print(f"Error reading file {file}: {e}")
        
        if not dataframes:
            raise ValueError(f"No valid CSV files found in directory: {directory}")
        
        concatenated_df = pl.concat(dataframes)
        return concatenated_df



    def validate_dataframes(self, df1, df2):
        # Ottieni i nomi delle colonne senza risolvere completamente lo schema
        df1_columns = df1.collect().columns
        df2_columns = df2.collect().columns

        if self.column_to_join not in df1_columns or self.column_to_join not in df2_columns:
            raise ValueError(f"Column {self.column_to_join} is not present in both DataFrames.")


    def join_dataframes(self, df1, df2):
        joined_df = df1.join(df2, on=self.column_to_join, how="inner")
        return joined_df 

    def add_provenance(self, df):
        prov_agent_url = "https://academictorrents.com/details/d9e554f4f0c3047d9f49e448a7004f7aa1701b69"
        source_url = "https://doi.org/10.13003/8wx5k"
        current_timestamp = datetime.utcnow().isoformat() + "Z"

        df = df.with_columns([
            pl.lit(prov_agent_url).alias('prov_agent'),
            pl.lit(source_url).alias('source'),
            pl.lit(current_timestamp).alias('prov_date')
        ])
        return df



class Delta:
    def __init__(self, df):
        self.df = df

    @staticmethod
    def contains_years(date_str):
        return len(date_str) >= 4

    @staticmethod
    def contains_months(date_str):
        return len(date_str) >= 7

    @staticmethod
    def contains_days(date_str):
        return len(date_str) >= 10

    @staticmethod
    def get_iso8601_duration(delta):
        duration = "P"
        if delta.years:
            duration += f"{abs(delta.years)}Y"
        if delta.months:
            duration += f"{abs(delta.months)}M"
        if delta.days:
            duration += f"{abs(delta.days)}D"
        if duration == "P":
            duration += "0D"
        return duration

    def calculate_date_difference(self, citing_date, cited_date):
        default_date = datetime(1900, 1, 1)
        
        if self.contains_years(cited_date) and self.contains_years(citing_date):
            citing_pub_datetime = parse(citing_date[:10], default=default_date)
            cited_pub_datetime = parse(cited_date[:10], default=default_date)
            delta = relativedelta(citing_pub_datetime, cited_pub_datetime)

            iso8601_duration = self.get_iso8601_duration(delta)
            if citing_pub_datetime < cited_pub_datetime: #controlla se il delta è negativo e mette il "-" davanti la P
                iso8601_duration = f"-{iso8601_duration}"
            return iso8601_duration
        else:
            return "Invalid dates"

    def add_delta_column(self):
        self.df = self.df.with_columns(
            pl.struct(["citing_date", "cited_date"]).map_elements(
                lambda x: self.calculate_date_difference(x["citing_date"], x["cited_date"]),
                return_dtype=pl.Utf8
            ).alias("time_span")
        )


    def save_csv(self, output_csv):
        self.df.sink_csv(output_csv)
        print(f"CSV with Delta column saved as {output_csv}")