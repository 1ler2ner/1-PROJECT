import pandas as pd

# ---------------------------
# Загрузка данных из Excel
# ---------------------------

# Чтение файла с минутными значениями напряжения
# skiprows=4 — пропускаем первые 4 строки (служебная информация)
# decimal="," — числа записаны с запятой как разделителем
data = pd.read_excel(
    r"D:\ALEX\ICS\U_1min_2025.xlsx",
    sheet_name="10.25",
    skiprows=4,
    decimal=","
)

# ---------------------------
# Выбор нужных столбцов
# ---------------------------

# Из исходной таблицы берём:
# Unnamed: 2  — метка времени
# Unnamed: 8  — напряжение линии BALTI–STRASENI
balti_str = pd.DataFrame({
    "timestamp": data["Unnamed: 2"],
    "BALTI-STRASENI": data["Unnamed: 8"]
})

# Устанавливаем колонку времени как индекс (для удобства анализа временного ряда)
balti_str = balti_str.set_index("timestamp")

# Разрешаем вывод всех строк (осторожно при больших объёмах данных)
pd.set_option("display.max_rows", None)

# Вывод первых 10 строк для проверки корректности загрузки
print(balti_str.head(10))

# ---------------------------
# Поиск максимального значения напряжения
# ---------------------------

# Максимальное значение напряжения
max_value = balti_str["BALTI-STRASENI"].max()

# Время, когда был зафиксирован максимум
max_time = balti_str["BALTI-STRASENI"].idxmax()

print("Max value:", max_value)
print("At time:", max_time)

# ---------------------------
# Статистический анализ
# ---------------------------

# Краткая статистика:
# count, mean, std, min, 25%, 50%, 75%, max
print(balti_str["BALTI-STRASENI"].describe())

# ---------------------------
# Проверка превышения 110% от номинала
# ---------------------------

Unom = 330                 # Номинальное напряжение, кВ
threshold = 1.1 * Unom     # Допустимый предел (110%)

# Отбор всех значений, превышающих допустимый предел
exceed = balti_str[balti_str["BALTI-STRASENI"] > threshold]

# Вывод первых случаев превышения
print(exceed.head())

# Количество превышений
print("Number of exceedances:", len(exceed))