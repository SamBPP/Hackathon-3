import pandas as pd
import datetime
from pipeline import utils

def clean_daytime_sheets(sheets, columns_names):
    """Processing all sheets and concatenate them together."""
    all_sheets = []
    for k in sheets.keys():
        month, year = utils.get_month_year(k)
        df = sheets[k]
        # change column names
        df.columns = columns_names
        # drop first two rows and any empties
        df = utils.drop_rows(df, 2)
        df = utils.drop_empty_rows(df)
        # clean columns
        for col in df.columns:
            if col != 'date':
                df[col] = df[col].apply(utils.clean_columns)
        for col in ['sunrise', 'sunset', 'solar_noon_time']:
            df[col] = df[col].str.split('(', expand=True).loc[:,0]
        # update date
        df['date'] = pd.to_datetime(df['date'].apply(lambda row: datetime.date(year, month, row)))
        all_sheets.append(df)
    return pd.concat(all_sheets, ignore_index=True)

def clean_weather_sheets(sheets, columns_names, drop_n, drop_start=True):
    all_sheets = []
    for k in sheets.keys():
        month, year = utils.get_month_year(k)
        df = sheets[k]
        # change column names
        df.columns = columns_names
        # drop rows and any empties
        if drop_start:
            df = utils.drop_rows(df, drop_n)
        else:
            df = utils.drop_rows(df, drop_n, len(df) - drop_n)
        df = utils.drop_empty_rows(df)
        # define snow column
        df = utils.define_snow(df)
        # clean columns
        for col in df.columns:
            if col not in ['date', 'wind_direction']:
                df[col] = df[col].apply(utils.clean_columns)
        # update date
        df['date'] = pd.to_datetime(df['date'].apply(lambda row: datetime.date(year, month, row)))
        all_sheets.append(df)
    return pd.concat(all_sheets, ignore_index=True)