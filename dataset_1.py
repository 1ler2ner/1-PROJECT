import pandas as pd

data = pd.read_excel(
    r"D:\ALEX\ICS\U_1min_2025.xlsx",
    sheet_name="10.25",
    skiprows=4,
    decimal=","
)

balti_str = pd.DataFrame({
    "timestamp": data["Unnamed: 2"],
    "BALTI-STRASENI": data["Unnamed: 8"]
})

balti_str = balti_str.set_index("timestamp")

pd.set_option("display.max_rows", None)
print(balti_str.head(10))

max_time = balti_str["BALTI-STRASENI"].idxmax()
max_value = balti_str["BALTI-STRASENI"].max()

print("Max value:", max_value)
print("At time:", max_time)

print(balti_str["BALTI-STRASENI"].describe())

Unom = 330
threshold = 1.1 * Unom

exceed = balti_str[balti_str["BALTI-STRASENI"] > threshold]

print(exceed.head())
print("Number of exceedances:", len(exceed))
