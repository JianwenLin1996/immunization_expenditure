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


def process_comparator_country_data(
    df: pd.DataFrame, column_name: str, filename: str, years: list
):
    ### COMPARATOR ###
    comparator_df = (
        df.sort_values(["year"], ascending=True)
        .groupby(["WHO_region", "year"])[column_name]
        .mean()
    )
    comparator_dict = {}
    for (region, year), col in comparator_df.items():
        if region not in comparator_dict:
            comparator_dict[region] = {}
        if year in years:
            comparator_dict[region][year] = nan_or_round(col)
    # comparator_json_str = json.dumps(comparator_dict, indent=4)
    with open(os.path.join("whdh_gold", f"{filename}_region.json"), "w") as json_file:
        json.dump(comparator_dict, json_file, indent=4)

    ### COUNTRY ###
    country_df = (
        df.sort_values(["year"], ascending=True)
        .groupby(["country", "year"])[column_name]
        .mean()
    )

    default_dict = {}
    for year in years:
        default_dict[year] = np.nan

    country_dict = {}
    for (country, year), col in country_df.items():
        if country not in country_dict:
            country_dict[country] = (
                default_dict.copy()
            )  # need copy() or else all country share the same dict
        if year in years:
            country_dict[country][year] = nan_or_round(col)
    # country_json_str = json.dumps(country_dict, indent=4)
    with open(os.path.join("whdh_gold", f"{filename}_country.json"), "w") as json_file:
        json.dump(country_dict, json_file, indent=4)


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

    years = [y for y in range(2018, 2024)]
    process_comparator_country_data(immune_exp_df, "TEV", "vaccine_spent", years)
    return


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

    years = [y for y in range(2018, 2024)]  #! year should be made flexible
    process_comparator_country_data(risk_opportunity_df, "BOP", "bop", years)
    return


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

    years = [y for y in range(2023, 2030)]  #! year should be made flexible
    process_comparator_country_data(fiscal_distribution_df, "USD", "usd", years)
    return


def gghed_gge_process():
    ad_coverages_df = parquet_to_df("MT_AD_IA2030")
    ad_coverages_df = (
        ad_coverages_df[ad_coverages_df["TYPE"].isin(["TERI", "TEV", "GERI", "GEV"])]
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
                "VALUE": "surviving_infant",
            }
        )
    )
    ref_finance_df = parquet_to_df("REF_FINANCING")
    ref_finance_df = (
        ref_finance_df[
            ref_finance_df["INDCODE"].isin(
                [
                    "LP",
                    "NGDPD",
                    "CHE_USD",
                    "PHC_USD",
                    "GGHED_USD",
                    "EXT_USD",
                    "GGHED_GGE",
                ]
            )
        ]
        .loc[:, ["COUNTRY", "YEAR", "INDCODE", "VALUE"]]
        .rename(
            columns={
                "COUNTRY": "country_code",
                "YEAR": "year",
                "VALUE": "value",
                "INDCODE": "code",
            }
        )
    )

    df = pd.merge(ad_coverages_df, ref_pop_df, on=["country_code", "year"], how="left")
    df = pd.merge(df, ref_finance_df, on=["country_code", "year"], how="left")
    df = (
        df.astype({"year": int})
        .replace(update_names.keys(), update_names.values())
        .round(3)
    )

    pivot_df_1 = (
        pd.pivot_table(df, values="value", index=["country", "year"], columns="code")
        .fillna(0)
        .reset_index()
    )
    pivot_df_2 = (
        pd.pivot_table(
            df, values="expenditure", index=["country", "year"], columns="vaccine"
        )
        .fillna(0)
        .reset_index()
    )

    teri_df = pivot_df_2[["country", "year", "GERI", "TERI"]]
    teri_df = teri_df.copy()
    teri_df.loc[:, "vaccine"] = "TERI"

    tev_df = pivot_df_2[["country", "year", "GEV", "TEV"]]
    tev_df = tev_df.copy()
    tev_df.loc[:, "vaccine"] = "TEV"

    tmp_df = teri_df.merge(tev_df, on=["country", "year"])

    teri_df = teri_df.rename(columns={"GERI": "GE", "TERI": "TE"})
    tev_df = tev_df.rename(columns={"GEV": "GE", "TEV": "TE"})

    tmp_df["B"] = tmp_df["TEV"] / tmp_df["TERI"]
    tmp_df = tmp_df[["country", "year", "B"]]

    te_df = pd.concat([teri_df, tev_df], ignore_index=True)
    new_df = pivot_df_1.merge(te_df, on=["country", "year"]).merge(
        df[
            [
                "country_code",
                "year",
                "country",
                "WHO_region",
                "GAVI",
                "surviving_infant",
            ]
        ].drop_duplicates(),
        on=["country", "year"],
    )
    new_df = new_df.merge(tmp_df, on=["country", "year"])

    # # new_df.fillna(0)
    new_df["C"] = new_df["TE"] / new_df["surviving_infant"]
    new_df["D"] = new_df["TE"] / new_df["LP"]
    new_df["E"] = new_df["GE"] / new_df["TE"]
    new_df["F"] = new_df["TE"] / new_df["NGDPD"]
    new_df["G"] = new_df["TE"] / new_df["CHE_USD"]
    new_df["H"] = new_df["TE"] / new_df["PHC_USD"]
    new_df["I"] = new_df["TE"] / new_df["GGHED_USD"]
    new_df["J"] = new_df["TE"] / new_df["EXT_USD"]
    new_df = new_df.replace([np.inf, -np.inf], np.nan)
    new_df = (
        new_df.astype({"year": int})
        .replace(update_names.keys(), update_names.values())
        .round(3)
    )

    years = [y for y in range(2018, 2022)]
    gghed_gge_df = (
        new_df[["country", "country_code", "WHO_region", "GAVI", "year", "GGHED_GGE"]]
        .drop_duplicates()
        .sort_values(by="year")
    )

    process_comparator_country_data(gghed_gge_df, "GGHED_GGE", "gghed_gge", years)
    return


def fin_sus_process():
    df = parquet_to_df("MT_AD_IA2030")
    df = (
        df[df["TYPE"].isin(["GEV", "TEV"])]
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
    gev_df = df.loc[
        df["vaccine"] == "GEV",
        ["country_code", "year", "vaccine", "expenditure"],
    ].rename(columns={"expenditure": "expenditure_gev"})
    tev_df = df[df["vaccine"] == "TEV"].rename(
        columns={"expenditure": "expenditure_tev"}
    )
    df = pd.merge(tev_df, gev_df, on=["country_code", "year"])
    df["gev/tev"] = df["expenditure_gev"] / df["expenditure_tev"]
    df.replace([np.inf, -np.inf], 0, inplace=True)

    conditions = [
        df["gev/tev"].isna(),
        df["gev/tev"] <= 0.2,
        (df["gev/tev"] > 0.2) & (df["gev/tev"] <= 0.4),
        (df["gev/tev"] > 0.4) & (df["gev/tev"] <= 0.6),
        (df["gev/tev"] > 0.6) & (df["gev/tev"] <= 0.8),
        df["gev/tev"] > 0.8,
    ]
    df["group"] = np.select(conditions, [0, 1, 2, 3, 4, 5], default=0)
    df = df.astype({"year": int}).replace(update_names.keys(), update_names.values())
    df = df.round(4)

    years = [y for y in range(2018, 2024)]
    process_comparator_country_data(df, "gev/tev", "fin_sus", years)
    return


def main():
    pd.set_option("future.no_silent_downcasting", True)
    pd.set_option("display.max_columns", None)
    vaccine_spent_process()
    risk_opportunity_process()
    fiscal_distribution_process()
    gghed_gge_process()
    fin_sus_process()

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
