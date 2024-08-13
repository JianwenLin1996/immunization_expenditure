import os
import pandas as pd


def process_GGX_MinusInterestPayments_LCU_index(df):
    who_base_year = 2023

    df["GGX_MinusInterestPayments_NGDP"] = df["GGX_NGDP"] - (
        df["GGXONLB_NGDP"] - df["GGXCNL_NGDP"]
    )
    df["GGX_MinusInterestPayments_NGDPRPC"] = (
        df["GGX_MinusInterestPayments_NGDP"] / 100
    ) * df["NGDPRPC"]

    temp = df[df["year"] >= who_base_year].copy()
    temp["GGX_MinusInterestPayments_LCU_index"] = temp.groupby("country_code")[
        "GGX_MinusInterestPayments_NGDPRPC"
    ].transform(lambda x: 100 * (x / x.iloc[0]))

    return temp[["country_code", "year", "GGX_MinusInterestPayments_LCU_index"]]


def process_GGX_MinusInterestPayments_ConstantUSD_percapita_rebased(df):
    # GGX_MinusInterestPayments_NGDPPC
    df["GGX_MinusInterestPayments_NGDP"] = df["GGX_NGDP"] - (
        df["GGXONLB_NGDP"] - df["GGXCNL_NGDP"]
    )
    df["GGX_MinusInterestPayments_NGDPPC"] = (
        df["GGX_MinusInterestPayments_NGDP"] / 100
    ) * df["NGDPPC"]

    # Rebaser_Coefficient
    rebase_year = 2024
    df["NGDP_D_Rebase"] = df.apply(
        lambda row: row["NGDP_D"] if row["year"] == rebase_year else pd.NA,
        axis=1,
    )
    df["NGDP_D_Rebase"] = df.groupby("country_code")["NGDP_D_Rebase"].ffill().bfill()
    df["Rebaser_Coefficient"] = df["NGDP_D_Rebase"] / df["NGDP_D"]

    # GGX_MinusInterestPayments_NCU_percapita_rebased
    df["GGX_MinusInterestPayments_NCU_percapita_rebased"] = (
        df["GGX_MinusInterestPayments_NGDPPC"] * df["Rebaser_Coefficient"]
    )

    # Implied_FX_Rebase
    df["Implied_FX"] = df["NGDPDPC"] / df["NGDPPC"]
    df["Implied_FX_Rebase"] = df.apply(
        lambda row: row["Implied_FX"] if row["year"] == rebase_year else pd.NA, axis=1
    )
    df["Implied_FX_Rebase"] = (
        df.groupby("country_code")["Implied_FX_Rebase"].ffill().bfill()
    )

    # GGX_MinusInterestPayments_ConstantUSD_percapita_rebased
    df["GGX_MinusInterestPayments_ConstantUSD_percapita_rebased"] = (
        df["GGX_MinusInterestPayments_NCU_percapita_rebased"] * df["Implied_FX_Rebase"]
    )

    return df[
        [
            "country_code",
            "year",
            "GGX_MinusInterestPayments_ConstantUSD_percapita_rebased",
        ]
    ]


def process_zerodose_dtpcv1():
    directory_path = "data"

    parquet_files = [
        os.path.join(directory_path, file)
        for file in os.listdir(directory_path)
        if file.startswith("AD_COVERAGES") and file.endswith(".parquet")
    ]

    temp_df = []
    # Iterate over the list of Parquet files and read each into a pandas DataFrame
    for file in parquet_files:
        df = pd.read_parquet(file)
        temp_df.append(df)

    ad_coverages_df = pd.concat(temp_df, ignore_index=True)

    who_dtpcv1year = 2022
    ad_coverages_df = ad_coverages_df[
        (ad_coverages_df["YEAR"] == who_dtpcv1year)
        & (ad_coverages_df["VACCINECODE"] == "DTPCV1")
        & (ad_coverages_df["COVERAGE_CATEGORY"] == "WUENIC")
    ]

    # Compute zerodose on just DTPCV1 VACCINECODE line
    ad_coverages_df["zerodose"] = round(
        ((100 - ad_coverages_df["PERCENTAGE"]) / 100) * ad_coverages_df["TARGETNUMBER"],
        0,
    )
    ad_coverages_df = ad_coverages_df[
        ["COUNTRY", "YEAR", "TARGETNUMBER", "PERCENTAGE", "zerodose"]
    ]

    # Rename 'PERCENTAGE' to 'DTPCV1'
    ad_coverages_df = ad_coverages_df.rename(
        columns={"COUNTRY": "country_code", "YEAR": "year", "PERCENTAGE": "DTPCV1"}
    )

    return ad_coverages_df


def main():
    pd.set_option("future.no_silent_downcasting", True)
    pd.set_option("display.max_columns", None)
    cy_imf_weo = pd.read_parquet("data/cy_imf_weo.parquet")

    lcu_df = process_GGX_MinusInterestPayments_LCU_index(df=cy_imf_weo)
    usd_df = process_GGX_MinusInterestPayments_ConstantUSD_percapita_rebased(
        df=cy_imf_weo
    )
    zerodose_df = process_zerodose_dtpcv1()

    df1 = pd.merge(lcu_df, usd_df, on=["country_code", "year"], how="outer")
    cy_ie_df = pd.merge(df1, zerodose_df, on=["country_code", "year"], how="outer")

    cy_ie_df.to_parquet(f"data/cy_ie.parquet", engine="pyarrow")


if __name__ == "__main__":
    main()
