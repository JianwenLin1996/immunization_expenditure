import pandas as pd
import numpy as np
import os


def process_files(filename):
    # read from file
    filepath = os.path.join("data/IMF_WEO_IN", filename)
    rcy_imf_weo = pd.read_csv(filepath, delimiter="\t", encoding="utf-16-le", dtype=str)

    # rename and create estimate_after
    rcy_imf_weo.rename(
        columns={
            "ISO": "country_code",
            "Estimates Start After": "estimate_after",
            "WEO Subject Code": "weo_subject_code",
        },
        inplace=True,
    )
    rcy_imf_weo = rcy_imf_weo[rcy_imf_weo["country_code"] != ""]
    rcy_imf_weo_estimate_after = rcy_imf_weo[
        ["country_code", "weo_subject_code", "estimate_after"]
    ]

    # drop irrelevant columns
    unnamed_columns = [col for col in rcy_imf_weo.columns if "Unnamed" in col]
    drop_years = [str(year) for year in range(1980, 2011)]
    rcy_imf_weo = rcy_imf_weo.drop(
        columns=[
            "WEO Country Code",
            "Country",
            "Subject Descriptor",
            "Subject Notes",
            "Country/Series-specific Notes",
            "Units",
            "Scale",
            "estimate_after",
        ]
        + unnamed_columns
        + drop_years
    )

    # replace NA n/a --
    rcy_imf_weo.replace({"n/a": np.nan, "NA": np.nan, "--": "0"}, inplace=True)
    for col in rcy_imf_weo.columns[2:]:
        rcy_imf_weo[col] = rcy_imf_weo[col].str.replace(",", "", regex=False)
        rcy_imf_weo[col] = pd.to_numeric(rcy_imf_weo[col], errors="coerce")

    # melt and merge with estimate_after
    rcy_imf_weo = rcy_imf_weo.melt(
        id_vars=["country_code", "weo_subject_code"],
        var_name="year",
        value_name="value",
    )
    rcy_imf_weo["year"] = pd.to_numeric(
        rcy_imf_weo["year"], errors="coerce", downcast="integer"
    )
    rcy_imf_weo = rcy_imf_weo.merge(
        rcy_imf_weo_estimate_after,
        on=["country_code", "weo_subject_code"],
        how="left",
    )

    # remove estimated row
    rcy_imf_weo["estimate_after"] = pd.to_numeric(
        rcy_imf_weo["estimate_after"], errors="coerce", downcast="integer"
    )
    # rcy_imf_weo["is_an_estimate"] = (~rcy_imf_weo["estimate_after"].isna()) & (
    #     rcy_imf_weo["year"] > rcy_imf_weo["estimate_after"]
    # )
    rcy_imf_weo["is_an_estimate"] = (~rcy_imf_weo["estimate_after"].isna()) & (
        rcy_imf_weo["year"] > rcy_imf_weo["estimate_after"]
    )
    # rcy_imf_weo = rcy_imf_weo[rcy_imf_weo["is_an_estimate"]] # not sure why estimate column is needed, since filter will be performed in imf_weo_analysis.py anyway
    rcy_imf_weo = rcy_imf_weo.drop(columns=["estimate_after", "is_an_estimate"])

    # pivot and save
    rcy_imf_weo["country_code"] = rcy_imf_weo["country_code"].astype(str)
    rcy_imf_weo["weo_subject_code"] = rcy_imf_weo["weo_subject_code"].astype(str)
    rcy_imf_weo = pd.pivot_table(
        rcy_imf_weo,
        values="value",
        index=["country_code", "year"],
        columns=["weo_subject_code"],
        aggfunc="mean",
    ).reset_index()
    rcy_imf_weo = rcy_imf_weo.sort_values(by=["country_code", "year"])
    rcy_imf_weo.to_parquet("data/cy_imf_weo.parquet", engine="pyarrow")
    return


def main():
    pd.set_option("display.max_columns", None)
    process_files("WEOApr2024all.xls")
    return 0


if __name__ == "__main__":
    main()
