import io
import pandas as pd

def to_excel_bytes(df: pd.DataFrame, sheet_name="RESULTADO") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()
