import pandas as pd

def load_sheets(file_path):
    """Load Excel workbook with all sheets."""
    return pd.read_excel(file_path, sheet_name=None)