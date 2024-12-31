import pandas as pd
import numpy as np

from src.conversions import *
# import models

def main():
    try:
        df = df_from_parquet('./data/table.parquet')
    except FileNotFoundError:
        print("Couldn't find that file.")
        return

    print(df.loc[:, ['Name', "Transfer Value"]])

if __name__ == "__main__":
    main()