import sys
import os
import json
import time
import math
import numpy as np
import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "packages"))
)


def csv_to_parquet(prefix):

    directory_path = "data"
    parquet_files = [
        os.path.join(directory_path, file)
        for file in os.listdir(directory_path)
        if file.startswith(prefix) and file.endswith(".parquet")
    ]

    temp_df = []
    # Iterate over the list of Parquet files and read each into a pandas DataFrame
    for file in parquet_files:
        df = pd.read_parquet(file)
        temp_df.append(df)

    concat_df = pd.concat(temp_df, ignore_index=True)
    return concat_df


def nan_or_round(val, multiply100=False, round_val=2):
    return (
        np.nan
        if math.isnan(val)
        else round(val * (100 if multiply100 else 1), round_val)
    )


def vaccine_spent_process():
    ad_coverages_df = csv_to_parquet("MT_AD_IA2030")
    ad_coverages_df = (
        ad_coverages_df[
            (ad_coverages_df["TYPE"] == "TEV") | (ad_coverages_df["TYPE"] == "TERI")
        ]
        .rename(
            columns={
                "COUNTRY": "country_code",
                "NAMEWORKEN": "country",
                "WHOREGIONC": "WHO_region",
                "GAVI_INCOME_STATUS": "GAVI",
                "YEAR": "year",
                "TYPE": "vaccine",
                "VALUE_TRANSFORMED": "expenditure",
            }
        )
        .loc[
            :,
            [
                "country_code",
                "country",
                "WHO_region",
                "GAVI",
                "year",
                "vaccine",
                "expenditure",
            ],
        ]
    )
    ref_pop_df = csv_to_parquet("REF_POPULATION")
    ref_pop_df = (
        ref_pop_df[
            (ref_pop_df["POP_SOURCE_FK"] == "UNPD2022")
            & (ref_pop_df["GENDER_FK"] == "BOTH")
            & (ref_pop_df["YEAR"] > 2010)
            & (ref_pop_df["POP_TYPE_FK"] == "SURVIVING_INFANT")
        ]
        .loc[:, ["COUNTRY_FK", "YEAR", "VALUE"]]
        .rename(
            columns={
                "COUNTRY_FK": "country_code",
                "YEAR": "year",
                "VALUE": "infant",
            }
        )
    )
    immune_exp_df = pd.merge(
        ad_coverages_df, ref_pop_df, on=["country_code", "year"], how="left"
    )
    immune_exp_df["expenditure_per_infant"] = (
        immune_exp_df["expenditure"] / immune_exp_df["infant"]
    )
    immune_exp_df = (
        immune_exp_df.astype({"year": int})
        .replace(update_names.keys(), update_names.values())
        .round(3)
    )

    dimension_df = immune_exp_df[
        ["country", "country_code", "WHO_region", "GAVI"]
    ].drop_duplicates()
    pivot_df = pd.pivot_table(
        immune_exp_df,
        values="expenditure_per_infant",
        index=["country", "year"],
        columns="vaccine",
    ).reset_index()
    immune_exp_df = pd.merge(dimension_df, pivot_df, on=["country"])
    immune_exp_df = immune_exp_df[
        (immune_exp_df["TEV"] != 0) & (immune_exp_df["TERI"] != 0)
    ]

    # merge gni
    current_gni_df = pd.read_csv("data/gni/current_gni.csv")[
        ["country_code", "year", "current_gni_usd"]
    ].astype({"year": int})
    constant_gni_df = pd.read_csv("data/gni/constant_gni_2015.csv")[
        ["country_code", "year", "constant_gni_2015_usd"]
    ].astype({"year": int})
    immune_exp_df = pd.merge(
        immune_exp_df, current_gni_df, on=["country_code", "year"], how="left"
    )
    immune_exp_df = pd.merge(
        immune_exp_df, constant_gni_df, on=["country_code", "year"], how="left"
    )

    ### COMPARATOR ###
    col_1_vaccine_spent_b_comparator_df = (
        immune_exp_df.sort_values(["year"], ascending=True)
        .groupby(["WHO_region", "year"])["TEV"]
        .mean()
    )
    comparator_dict = {}
    for (region, year), mean_tev in col_1_vaccine_spent_b_comparator_df.items():
        if region not in comparator_dict:
            comparator_dict[region] = {}
        comparator_dict[region][year] = nan_or_round(mean_tev)
    # comparator_json_str = json.dumps(comparator_dict, indent=4)
    with open(os.path.join("whdh_gold", "vaccine_spent_region.json"), "w") as json_file:
        json.dump(comparator_dict, json_file, indent=4)

    ### COUNTRY ###
    col_1_vaccine_spent_b_country_df = (
        immune_exp_df.sort_values(["year"], ascending=True)
        .groupby(["country", "year"])["TEV"]
        .mean()
    )

    default_dict = {}
    for year in range(2018, 2024):  #! year should be made flexible
        default_dict[year] = np.nan

    country_dict = {}
    for (country, year), tev in col_1_vaccine_spent_b_country_df.items():
        if country not in country_dict:
            country_dict[country] = (
                default_dict.copy()
            )  # need this or else all country share the same dict
        country_dict[country][year] = nan_or_round(tev)
    # country_json_str = json.dumps(country_dict, indent=4)
    with open(
        os.path.join("whdh_gold", "vaccine_spent_country.json"), "w"
    ) as json_file:
        json.dump(country_dict, json_file, indent=4)


def main():
    pd.set_option("future.no_silent_downcasting", True)
    pd.set_option("display.max_columns", None)
    vaccine_spent_process()

    return 0


if __name__ == "__main__":

    update_names = {
        "Gavi low income": "Low Income",
        "non-Gavi middle income": "MIC (GAVI ineligible)",
        "High income": "High Income",
        "Gavi low-middle income": "MIC (GAVI eligible)",
        "AFRO": "African Region",
        "AMRO": "Americas Region",
        "EMRO": "Eastern Mediterranean Region",
        "EURO": "European Region",
        "SEARO": "South-East Asian Region",
        "WPRO": "Western Pacific Region",
        "AFR": "African Region",
        "AMR": "Americas Region",
        "EMR": "Eastern Mediterranean Region",
        "EUR": "European Region",
        "SEAR": "South-East Asian Region",
        "WPR": "Western Pacific Region",
    }
    main()
