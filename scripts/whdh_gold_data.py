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


def parquet_to_df(prefix):

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
    ad_coverages_df = parquet_to_df("MT_AD_IA2030")
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
    ref_pop_df = parquet_to_df("REF_POPULATION")
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


def risk_opportunity_process():
    ad_coverages_df = parquet_to_df("MT_AD_IA2030")
    ad_coverages_df = ad_coverages_df.rename(
        columns={
            "COUNTRY": "country_code",
            "NAMEWORKEN": "country",
            "WHOREGIONC": "WHO_region",
            "GAVI_INCOME_STATUS": "GAVI",
            "YEAR": "year",
        }
    ).loc[
        :,
        ["country_code", "country", "WHO_region", "GAVI", "year"],
    ]
    bop_df = parquet_to_df("V_AD_COV_BOP")
    bop_df = bop_df.rename(
        columns={
            "NAME": "country",
            "YEAR": "year",
        }
    ).loc[:, ["country", "year", "BOP"]]
    c_ie_df = pd.read_parquet(os.path.join("data", "cy_ie.parquet"))
    c_ie_df = (
        c_ie_df[c_ie_df["year"] == 2024]
        .rename(columns={"GGX_MinusInterestPayments_LCU_index": "LCU"})
        .loc[:, ["country_code", "year", "LCU"]]
    )

    # merge df
    risk_opportunity_df = pd.merge(
        ad_coverages_df, bop_df, on=["country", "year"], how="left"
    )
    risk_opportunity_df = pd.merge(
        risk_opportunity_df, c_ie_df, on=["country_code", "year"], how="left"
    )

    risk_opportunity_df = risk_opportunity_df.groupby(["country", "year"]).filter(
        lambda x: len(x) > 1
    )
    risk_opportunity_df = (
        risk_opportunity_df.astype({"year": int})
        .replace(update_names.keys(), update_names.values())
        .round(3)
    )

    ### COMPARATOR ###
    col_1_risk_oppo_b_comparator_BOP_df = (
        risk_opportunity_df.sort_values(["year"], ascending=True)
        .groupby(["WHO_region", "year"])["BOP"]
        .mean()
    )
    comparator_dict = {}
    for (region, year), bop in col_1_risk_oppo_b_comparator_BOP_df.items():
        if region not in comparator_dict:
            comparator_dict[region] = {}
        comparator_dict[region][year] = nan_or_round(bop)
    # comparator_json_str = json.dumps(comparator_dict, indent=4)
    with open(os.path.join("whdh_gold", "bop_region.json"), "w") as json_file:
        json.dump(comparator_dict, json_file, indent=4)

    ### COUNTRY ###
    col_1_risk_oppo_b_country_BOP_df = (
        risk_opportunity_df.sort_values(["year"], ascending=True)
        .groupby(["country", "year"])["BOP"]
        .mean()
    )

    default_dict = {}
    for year in range(2018, 2024):  #! year should be made flexible
        default_dict[year] = np.nan

    country_dict = {}
    for (country, year), bop in col_1_risk_oppo_b_country_BOP_df.items():
        if country not in country_dict:
            country_dict[country] = (
                default_dict.copy()
            )  # need this or else all country share the same dict
        country_dict[country][year] = nan_or_round(bop)
    # country_json_str = json.dumps(country_dict, indent=4)
    with open(os.path.join("whdh_gold", "bop_country.json"), "w") as json_file:
        json.dump(country_dict, json_file, indent=4)


def fiscal_distribution_process():
    country_df = parquet_to_df("MT_AD_IA2030")
    country_df = (
        country_df.rename(
            columns={
                "COUNTRY": "country_code",
                "NAMEWORKEN": "country",
                "WHOREGIONC": "WHO_region",
                "GAVI_INCOME_STATUS": "GAVI",
            }
        )
        .loc[
            :,
            ["country_code", "country", "WHO_region", "GAVI"],
        ]
        .drop_duplicates()
    )
    cy_ie_df = pd.read_parquet(os.path.join("data", "cy_ie.parquet"))
    cy_ie_df = cy_ie_df.rename(
        columns={
            "GGX_MinusInterestPayments_LCU_index": "LCU",
            "GGX_MinusInterestPayments_ConstantUSD_percapita_rebased": "USD",
        }
    ).loc[
        :,
        ["country_code", "year", "LCU", "USD"],
    ]

    fiscal_distribution_df = pd.merge(
        country_df, cy_ie_df, on=["country_code"], how="left"
    )
    fiscal_distribution_df = (
        fiscal_distribution_df.astype({"year": int})
        .replace(update_names.keys(), update_names.values())
        .round(3)
    )

    threshold_middle = 100
    threshold_width = 5
    threshold_1 = threshold_middle + (-2 * threshold_width)
    threshold_2 = threshold_middle + (-1 * threshold_width)
    threshold_3 = threshold_middle + (1 * threshold_width)
    threshold_4 = threshold_middle + (2 * threshold_width)

    conditions = [
        fiscal_distribution_df["LCU"].isna(),
        fiscal_distribution_df["LCU"] <= threshold_2,
        (fiscal_distribution_df["LCU"] > threshold_2)
        & (fiscal_distribution_df["LCU"] <= threshold_3),
        fiscal_distribution_df["LCU"] > threshold_3,
    ]
    fiscal_distribution_df["group"] = np.select(conditions, [0, 1, 3, 5], default=5)

    #! post process of fiscal distribution for col_1_risk_oppo_b_country_USD


def main():
    pd.set_option("future.no_silent_downcasting", True)
    pd.set_option("display.max_columns", None)
    # vaccine_spent_process()
    # risk_opportunity_process()
    fiscal_distribution_process()

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
