import pandas as pd

def read_excel_any(file):
    df = pd.read_excel(file)
    df.columns = df.columns.astype(str).str.upper().str.strip()
    return df
