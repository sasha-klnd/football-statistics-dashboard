import pandas as pd
import numpy as np
import pyarrow as pya
import pyarrow.parquet as pq

def fbm_html_to_parquet(file_path, output_name):
    # Read df
    df = pd.read_html(file_path, encoding='utf-8')[0]

    # Cleaning
    df.loc[:, "Transfer Value"] = df.loc[:, "Transfer Value"].str.replace("€", "")
    df.loc[:, "Transfer Value"] = df.loc[:, "Transfer Value"].str.replace("K", "000")
    df.loc[:, "Transfer Value"] = df.loc[:, "Transfer Value"].str.replace("M", "000000")
    df["Transfer Value"] = np.where(df["Transfer Value"] == "Unknown", None, df["Transfer Value"])
    df["Transfer Value Lower"] = df["Transfer Value"].str.split(" ").str[0]
    df["Transfer Value Upper"] = df["Transfer Value"].str.split(" ").str[2]

    # Write to parquet
    table = pya.Table.from_pandas(df)

    if ".parquet" in output_name:
        pq.write_table(table, f"./data/{output_name}")
    elif "." in output_name:
        print("Unsupported file type.")
        return
    else:
        pq.write_table(table, f"./data/{output_name}.parquet")


def fbref_html_to_parquet(file_path, output_name):
    # Do later
    pass

def df_from_parquet(file_path):
    table = pq.read_table(file_path)
    df = table.to_pandas()
    return df

fbm_html_to_parquet("./data/table.html", "table")