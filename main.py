import pandas as pd
import numpy as np
import re

from src.conversions import *
# import models

def main():

    # clean_fbm_parquet("./data/table.parquet", "clean_table")

    table = pq.read_table("./data/table.parquet")
    df = table.to_pandas()

    df["Transfer Value"] = df["Transfer Value"].str.replace("€", "")
    

    df["TVLower"] = np.where(df['Transfer Value'] == "Unknown", '-1', df["Transfer Value"].str.split(" ").str[0])
    print(df.loc[df["TVLower"].str.contains(r'[.]\d+[M]'), ['Name', "TVLower"]])

    # df["TVUpper"] = np.where(df['Transfer Value'] == "Unknown", '-1', df["Transfer Value"].str.split(" ").str[2])
    # df_dot_tv = df.loc[df['Transfer Value'].str.contains(r'\d+[.]\d', na=False)]
    # print(df.loc[:, ['Name', "TVLower", "TVUpper"]])

    # df.loc[:, "Transfer Value"] = df.loc[:, "Transfer Value"].str.replace("M", "000000")
    # df["Transfer Value"] = np.where(df["Transfer Value"] == "Unknown", '-1', df["Transfer Value"])
    # df.drop(columns=['Transfer Value'])
    
    # print(df.loc[df["TV Lower"] == '2.5000', ['TV Lower']])

    # print(df.astype({'TV Lower': 'int32'})['TV Lower'].dtypes)

    # df = df_from_parquet("./data/table.parquet")
    # print(df['Transfer Value Lower'].dtypes)

if __name__ == "__main__":
    main()