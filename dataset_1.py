
import pandas as pd

data = pd.read_excel(r"D:\ALEX\ICS\U_1min_2025.xlsx", sheet_name="10.25",decimal=",")
print(data.columns)
print(data.shape)

print(data.head(10))
