import typing

import pandas as pd
from loguru import logger
from tqdm.auto import tqdm


class StandardTimeProcessor(object):
    """
    Calculate standard time from production event message table using Bootstrap.

    Attributes
    ----------
    start_hours : typing.List
        start hour of each cycle with forecast hour list

        .. code-block:: python

            [
                {
                    "start_hour": "00",
                    "forecast_hours": [0, 1, 2, 3, ... ]
                }
            ]

    bootstrap_count: int
        sampling times.
    bootstrap_sample: int
        number of samples per time.
    quantile: float
        confidence interval size
    """
    def __init__(
            self,
            start_hours: typing.List,
            bootstrap_count: int = 1000,
            bootstrap_sample: int = 10,
            quantile: float = 0.99,
            show_progress: bool = False,
    ):
        self.start_hours = start_hours
        self.bootstrap_count = bootstrap_count
        self.bootstrap_sample = bootstrap_sample
        self.quantile = quantile
        self.show_progress = show_progress

    def process_data(
            self,
            table: pd.DataFrame,
    ) -> typing.List:
        table["start_hour"] = table["start_time"].apply(lambda x: x.strftime("%H"))
        table["clock"] = table["time"] - table["start_time"]

        times = []

        for item in self.start_hours:
            start_hour = item["start_hour"]
            forecast_hours = item["forecast_hours"]
            df_start_hour = table[table["start_hour"] == start_hour][["forecast_hour", "clock"]]
            standard_times = []

            if self.show_progress:
                forecast_hours_range = tqdm(range(len(forecast_hours)), desc=f"{start_hour} hour loop")
            else:
                forecast_hours_range = forecast_hours

            for hour_index in forecast_hours_range:
                forecast_hour = forecast_hours[hour_index]
                clock_df_hour = df_start_hour[df_start_hour["forecast_hour"] == forecast_hour]

                means = self._get_means(
                    clock_df_hour,
                    bootstrap_sample=self.bootstrap_sample,
                    bootstrap_count=self.bootstrap_count,
                )

                standard_time = self._get_bound(
                    means,
                    forecast_hour=forecast_hour,
                    quantile=self.quantile,
                )

                standard_times.append(standard_time)

            times.append({
                "start_hour": start_hour,
                "times": standard_times,
            })

        prod_time_dfs = [
            pd.DataFrame(a_time["times"]) for a_time in times
        ]

        for df in prod_time_dfs:
            df["upper_duration"] = df["upper_bound"].apply(lambda x: x.isoformat())
            df["lower_duration"] = df["lower_bound"].apply(lambda x: x.isoformat())

        production_times = [
            {
                "start_hour": self.start_hours[index]["start_hour"],
                "times": df[["forecast_hour", "upper_duration", "lower_duration"]].to_dict("records")
            } for index, df in enumerate(prod_time_dfs)
        ]

        return production_times

    def _get_mean(
            self,
            clock_df: pd.DataFrame,
            bootstrap_sample: int,
    ) -> pd.Timedelta:
        sampled_data = clock_df["clock"].sample(
            n=bootstrap_sample,
            replace=True,
        )
        return sampled_data.mean()

    def _get_means(
            self,
            clock_df,
            bootstrap_sample,
            bootstrap_count
    ) -> typing.List[pd.Timedelta]:
        means = []
        for i in range(bootstrap_count):
            mean = self._get_mean(clock_df, bootstrap_sample)
            means.append(mean)
        return means

    def _get_bound(
            self,
            means: typing.List[pd.Timedelta],
            forecast_hour: int,
            quantile: float,
    ) -> typing.Dict:
        bdf = pd.DataFrame(means).applymap(lambda x: x.ceil("s"))
        upper_bound = bdf.quantile(
            quantile + (1 - quantile) / 2,
            numeric_only=False,
            interpolation="nearest"
        )
        lower_bound = bdf.quantile(
            (1 - quantile) / 2,
            numeric_only=False,
            interpolation="nearest",
        )
        return {
            "forecast_hour": forecast_hour,
            "upper_bound": upper_bound[0],
            "lower_bound": lower_bound[0],
        }
