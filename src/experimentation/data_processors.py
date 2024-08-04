import pandas as pd
import scipy.stats as stats
import numpy as np

from typing import List


class ABDataProcessor:
    """
    This class should be used to manipulate the data from the AB test in order to:
        1. Calculate the p-value
        2. Calculate the relative uplift confidence interval

    This class expects an aggregated dataframe with the following columns:
        - variant_name: the variant name
        - seg_name: the segment name
        - platform: the platform name (e.g. ios, android)
        - kpi: the metric name
        - identifier: platform || delimiter || variant_name || delimiter || seg_name
        - treatment_(mean | varaince | users): the mean, variance, and number of users in the treatment group
        - control_(mean | varaince | users): the mean, variance, and number of users in the control group
        - z_score: the z-score of the treatment group
        - pooled_variance: the pooled variance of the treatment and control groups used for p-value calculation TODO: Clarify this
        - _pooled_variance: the pooled variance of the treatment and control groups
        - treatment_uplift: the uplift of the treatment group

    """

    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()

    def calculate_p_value(self) -> pd.DataFrame:
        """
        This function calculates the p-value of the AB test.
        Assumes only a one-sided test.
        """
        self.df["P_VALUE"] = self.df.apply(
            lambda x: 1 - stats.norm.cdf(x["Z_SCORE"]),
            axis=1,
        )

        return self.df

    def is_statistically_significant(
        self, confidence_level: float = 0.9
    ) -> pd.DataFrame:
        """
        This function calculates whether the AB test is statistically significant.
        Assumes only a one-sided test.
        """
        self.df["IS_STATISTICALLY_SIGNIFICANT"] = self.df["P_VALUE"].apply(
            lambda x: x <= (1 - confidence_level)
        )
        return self.df

    def color_utility_func(self, x: pd.DataFrame) -> str:
        """
        Utility function used by the color_statistically_significant function
        """
        if x["IS_STATISTICALLY_SIGNIFICANT"] == False:
            return "#CFCFC4"
        elif x["Z_SCORE"] >= 0:
            return "#77DD77"
        else:
            return "#FF6961"

    def color_statistically_significant(self) -> pd.DataFrame:
        """
        This function colors the statistically significant rows
        """
        self.df["COLOR"] = self.df.apply(lambda x: self.color_utility_func(x), axis=1)
        return self.df

    def calculate_uplift_confidence_interval(
        self, confidence_level: float = 0.95
    ) -> pd.DataFrame:
        """
        This function calculates the confidence interval of the AB test.
        """
        ci_constant = stats.norm.ppf(confidence_level)

        self.df["CI_HALF_WIDTH"] = self.df.apply(
            lambda x: np.sqrt(
                x["_POOLED_VARIANCE"]
                * (1 / x["TREATMENT_USERS"] + 1 / x["CONTROL_USERS"])
            ),
            axis=1,
        )

        self.df["UPPER_CI"] = self.df.apply(
            lambda x: x["TREATMENT_MEAN"]
            - x["CONTROL_MEAN"]
            + ci_constant * x["CI_HALF_WIDTH"],
            axis=1,
        )
        self.df["LOWER_CI"] = self.df.apply(
            lambda x: x["TREATMENT_MEAN"]
            - x["CONTROL_MEAN"]
            - ci_constant * x["CI_HALF_WIDTH"],
            axis=1,
        )

        # Normalize the confidence interval to the control mean
        self.df["UPLIFT_UPPER_CI"] = self.df["UPPER_CI"] / self.df["CONTROL_MEAN"]
        self.df["UPLIFT_LOWER_CI"] = self.df["LOWER_CI"] / self.df["CONTROL_MEAN"]

        return self.df

    def round_cols(self, cols: List[str], dp: int = 4) -> pd.DataFrame:
        """
        Rounds the columns in the list
        """
        self.df[cols] = self.df[cols].round(dp)

        return self.df

    def process_data(self):
        """
        Combines all the functions above
        Each function also alters the dataframe in place
        """

        _ = self.calculate_p_value()
        _ = self.is_statistically_significant()
        _ = self.color_statistically_significant()
        _ = self.calculate_uplift_confidence_interval()
        _ = self.round_cols(["UPLIFT_LOWER_CI", "UPLIFT_UPPER_CI"])

        return self.df


class ExperimentSummaryStats:
    def __init__(self, df: pd.DataFrame, experiment_metrics: list[str]) -> None:
        self.df = df.copy()
        self.experiment_metrics = experiment_metrics

    def get_summary_statistics(
        self, experiment_df: pd.DataFrame, experiment_metrics: list[str]
    ):
        """
        Compute summary statistics of the experiment
            mean, variance, count

        For each metric and variant
        """
        # Get descriptive stats of metrics
        summary_df = (
            experiment_df.groupby(["VARIANT_NAME", "VARIANT_DEFAULT"])[
                experiment_metrics
            ]
            .agg(["mean", "var", "count"])
            .stack(future_stack=True)
            .reset_index()
            .rename(columns={"level_2": "STATISTIC"})
        )

        return summary_df

    def transform_summary_statistics(
        self, summary_df: pd.DataFrame, experiment_metrics: list[str]
    ) -> pd.DataFrame:
        """
        Coerce dataframe to correct shape
        """
        # TODO: Figure out if this step is necessary
        # Reshape dataframe to get metrics as columns
        melt_df = summary_df.melt(
            id_vars=["VARIANT_NAME", "VARIANT_DEFAULT", "STATISTIC"],
            value_vars=experiment_metrics,
        ).rename(columns={"variable": "KPI", "value": "VALUE"})

        # Get STATISTIC as columns
        experiment_kpi_df = melt_df.pivot(
            index=["VARIANT_NAME", "VARIANT_DEFAULT", "KPI"],
            columns="STATISTIC",
            values="VALUE",
        ).reset_index()

        return experiment_kpi_df

    def join_control_to_variant(self, experiment_kpi_df: pd.DataFrame) -> pd.DataFrame:
        """
        Join control to variant
        """

        control_df = experiment_kpi_df[experiment_kpi_df["VARIANT_DEFAULT"] == 1]
        variant_df = experiment_kpi_df[experiment_kpi_df["VARIANT_DEFAULT"] == 0]

        joined_experiment_kpi_df = pd.merge(
            left=variant_df,
            right=control_df[["KPI", "mean", "var", "count"]],
            on=["KPI"],
            how="left",
            suffixes=("_EXP", "_CONTROL"),
        )

        return joined_experiment_kpi_df

    def transform(self):
        """
        Transform dataframe to get summary statistics of each variant joined
        to control data
        """
        summary_df = self.get_summary_statistics(self.df, self.experiment_metrics)
        experiment_kpi_df = self.transform_summary_statistics(
            summary_df, self.experiment_metrics
        )
        joined_experiment_kpi_df = self.join_control_to_variant(experiment_kpi_df)

        return joined_experiment_kpi_df


class DFABTestProcessor:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df = df.copy()

    def get_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Given a dataframe of schema X
            Perform the necessary transformations to get the dataframe into the correct form
            for the z-test
        """

        df["POOLED_VARIANCE"] = (df["var_CONTROL"] / df["count_CONTROL"]) + (
            df["var_EXP"] / df["count_EXP"]
        )
        df["_POOLED_VARIANCE"] = (
            (df["var_CONTROL"] * (df["count_CONTROL"] - 1))
            + (df["var_EXP"] * (df["count_EXP"] - 1))
        ) / (df["count_CONTROL"] + df["count_EXP"] - 2)

        df["Z_SCORE"] = (df["mean_EXP"] - df["mean_CONTROL"]) / np.sqrt(
            df["POOLED_VARIANCE"]
        )

        df["TREATMENT_UPLIFT"] = (df["mean_EXP"] - df["mean_CONTROL"]) / df[
            "mean_CONTROL"
        ]

        return df

    def rename_df_cols(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recast dataframe columns to correct types
        """
        col_mapping_dict = {
            "var_EXP": "TREATMENT_VARIANCE",
            "var_CONTROL": "CONTROL_VARIANCE",
            "mean_EXP": "TREATMENT_MEAN",
            "mean_CONTROL": "CONTROL_MEAN",
            "count_EXP": "TREATMENT_USERS",
            "count_CONTROL": "CONTROL_USERS",
        }

        df = df.rename(columns=col_mapping_dict)

        return df

    def process_df(self) -> pd.DataFrame:
        """
        Combines all the functions above
        Each function also alters the dataframe in place
        """
        processed_df = self.get_metrics(self.df)
        processed_df = self.rename_df_cols(processed_df)

        return processed_df
