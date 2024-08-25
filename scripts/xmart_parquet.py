import sys
import os
import time
import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "packages"))
)
from xmart_extractor import XmartExtractor


def extract_from_api(xmart):

    # MT_AD_IA2030_FINANCING
    # SURVIVING_INFANT <- REF_POPULATIONS
    # V_AD_COV_BOP_LONG
    # REF_IA2030_FINANCING <- REF_FINANCING
    tables = [
        "REF_POPULATIONS",
        "V_AD_COV_BOP_LONG",
        "REF_FINANCING",
        "AD_COVERAGES",
        "MT_AD_IA2030_FINANCING",
    ]

    start_year = 2018
    end_year = 2025
    years = list(range(start_year, end_year + 1))
    # years_str = ",".join([str(year) for year in years])

    for year in years:
        chunk = 10000
        for table in tables:
            skip = 0
            top = chunk

            stop = False
            merged_df = None
            while not stop:
                start_time = time.time()
                response = xmart.get(
                    f"{table}?$top={top}&$skip={skip}&$filter=YEAR in ({str(year)})"
                )
                data = response["value"]
                print("--- %s seconds ---" % (time.time() - start_time))
                if len(data) == 0:
                    stop = True
                    continue
                else:
                    df = pd.json_normalize(data)
                    if merged_df is None:
                        merged_df = df
                    else:
                        merged_df = pd.concat([merged_df, df], ignore_index=True)
                    skip = skip + chunk
                    continue
            if merged_df is not None:
                print("done and saving as parquet")
                merged_df.to_parquet(
                    f"data/{table}_{str(year)}.parquet", engine="pyarrow"
                )


def main():
    pd.set_option("display.max_columns", None)
    xmart = XmartExtractor()
    extract_from_api(xmart)
    return 0


if __name__ == "__main__":
    main()
