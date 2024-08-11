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

    print(
        temp[["country_code", "year", "GGX_MinusInterestPayments_LCU_index"]].head(50)
    )
    return


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

    print(
        df[
            [
                "country_code",
                "year",
                "GGX_MinusInterestPayments_ConstantUSD_percapita_rebased",
            ]
        ].head(50)
    )


def process_zerodose(df):
    return


def main():
    pd.set_option("future.no_silent_downcasting", True)
    pd.set_option("display.max_columns", None)
    cy_imf_weo = pd.read_parquet("data/cy_imf_weo.parquet")

    # process_GGX_MinusInterestPayments_LCU_index(df)
    # process_GGX_MinusInterestPayments_ConstantUSD_percapita_rebased(df=cy_imf_weo)


if __name__ == "__main__":
    main()
