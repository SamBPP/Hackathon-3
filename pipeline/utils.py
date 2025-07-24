import re
import numpy as np

def drop_empty_rows(df, thresh=0.5):
    """Drop a row if at least "thresh"% is missing."""
    return df.dropna(thresh=int(thresh*len(df.columns))).reset_index(drop=True)

def clean_columns(x):
    """Clean columns by replacing non-ascii and 'bad' characters"""
    x = str(x).replace('−', '-')
    x = re.sub("[^().+-:\d]", '', x)
    return x

def get_month_year(yymm, century=20):
    """Get month and year from YYMM string"""
    month = int(yymm[2:])
    year = century*100 + int(yymm[:2])
    return month, year

def drop_rows(df, n, start=0):
    """Drop the leading n rows in a DataFrame"""
    return df.drop(index=range(start, start + n)).reset_index(drop=True)


def define_snow(df):
    """Takes snow data out of rain data."""
    df['snow_mm'] = np.where(df['rain_mm'].astype(str).str.endswith('s'),
                             df['rain_mm'],
                             None)
    df['rain_mm'] = np.where(df['rain_mm'].astype(str).str.endswith('s'),
                             None,
                             df['rain_mm'])
    return df

def merge_datasets(df1, df2):
    return df1.merge(df2, how='inner', on='date')

def get_mins_from_time(time):
    return 60*int(time.split(':')[0]) + int(time.split(':')[1])

def filter_by_year(df, year):
    return df[df['date'].dt.year == year].reset_index(drop=True)