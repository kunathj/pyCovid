# Code based on blog post: https://iscinumpy.gitlab.io/post/johns-hopkins-covid/.

from collections import defaultdict
import numpy as np
import os
import pandas as pd
from urllib.error import HTTPError


multi_index = ["Last_Update", "Country_Region", "Province_State", "Admin2"]

def download_day(day: pd.Timestamp):
    try:
        table = pd.read_csv(
            "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/"
            "master/csse_covid_19_data/csse_covid_19_daily_reports/"
            f"{day:%m-%d-%Y}.csv",
            na_values=['#VALUE!', '#DIV/0!'],
        )
    except HTTPError:
        return pd.DataFrame()

    # Cleanup - sadly, the format has changed a bit over time.
    # e can normalize that here.
    table.columns = [
        f.replace("/", "_")
        .replace(" ", "_")
        .replace("Case-Fatality_Ratio", "Case_Fatality_Ratio")
        .replace("Latitude", "Lat")
        .replace("Longitude", "Long_")
        .replace("Incidence_Rate", "Incident_Rate")
        for f in table.columns
    ]

    # This column is new in recent datasets.
    if "Admin2" not in table.columns:
        table["Admin2"] = None

    # New datasets have these, but they are not very useful for now.
    table.drop(columns=["FIPS", "Combined_Key", "Lat", "Long_"],
               errors="ignore", inplace=True
    )

    # If the last update time was useful, we would make this day only, r
    # rather than day + time:
    # table["Last_Update"] = pd.to_datetime(table["Last_Update"]).dt.normalize()
    # However, last update is odd, let's just make this the current day.
    table["Last_Update"] = day

    # Make sure indexes are not NaN, which causes later bits to not work.
    # 0 isn't perfect, but good enough.
    return table.fillna(0).set_index(multi_index, drop=True)


def get_day(day: pd.Timestamp):
    try:
        return pd.read_csv(f"data/{day:%m-%d-%Y}.csv").set_index(multi_index)
    except:
        print("Not a valid file:", f"data/{day:%m-%d-%Y}.csv")
        print("This is common behavior for today's (still empty) dataframe.")


def purge_last_days(n_purged=5):
    """Remove the latest days that were downloaded.

    I don't know how the JHU dataset is maintained. It seems reasonable that the
    last 1-2 days might get updates, e.g. from states that are late with their
    result publishing.
    To be on the safe side, trigger a re-downloading of the last few days by
    removing those files from the local storage.
    """
    if n_purged == 0:
        return # Prevent removing all files.
    if not os.path.isdir("data"):
        return
    for day_df in sorted(os.listdir("data"))[-n_purged:]:
        os.remove(f"data/{day_df}")


def store_all_days(end_day):
    purge_last_days()

    # Not needed here, created for convenience.
    os.makedirs("img", exist_ok=True)

    os.makedirs("data", exist_ok=True)
    for day in pd.date_range("2020-01-22", end_day):
        if os.path.exists(f"data/{day:%m-%d-%Y}.csv"):
            continue
        day_df = download_day(day)
        day_df.to_csv(f"data/{day:%m-%d-%Y}.csv")



def get_all_days(end_day=None):
    # Assume current day - 1 is the latest dataset if no end given.
    if end_day is None:
        end_day = pd.Timestamp.now().normalize()

    store_all_days(end_day)

    date_range_list = pd.date_range("2020-01-22", end_day)
    day_generator = (get_day(day) for day in date_range_list)

    as_types = defaultdict(int)
    for float_c in ["Incident_Rate", "Case_Fatality_Ratio"]:
        as_types[float_c] = float
    # Make a big dataframe, NaN is 0.
    df = pd.concat(day_generator).fillna(0).astype(as_types)

    # Remove a few duplicate keys.
    df = df.groupby(level=df.index.names).sum()

    # Sometimes active is not filled in; we can compute it easily.
    df["Active"] = np.clip(
        df["Confirmed"] - df["Deaths"] - df["Recovered"], 0, None
    )

    # Change in confirmed cases (placed in a pleasing location in the table).
    index_levels = ("Country_Region", "Province_State", "Admin2")
    df.insert(1, "ΔConfirmed",
              df.groupby(level=index_levels)["Confirmed"]
                .diff().fillna(0).astype(int),
    )

    # Similarly for deaths.
    df.insert(3, "ΔDeaths",
              df.groupby(level=index_levels)["Deaths"]
                .diff().fillna(0).astype(int),
    )
    return df