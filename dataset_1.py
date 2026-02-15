
import pandas as pd

data = pd.read_excel(
    r"D:\ALEX\ICS\U_1min_2025.xlsx",
    sheet_name="10.25",
    skiprows=4,
    decimal=","
)

pd.set_option("display.max_columns", None)
print(data.head())
print(data.shape)


