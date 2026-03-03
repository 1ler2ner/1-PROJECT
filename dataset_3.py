# -------------------------------------------------------------------------
# WARNING: DST (переход на зимнее время) — возможные дубли timestamp
#
# В период перевода часов (обычно октябрь) час 03:00–03:59 может повторяться.
# Это приводит к дублирующимся временным меткам (одинаковый timestamp дважды).
#
# В текущей версии скрипта дубли агрегируются (см. DEDUP_RULE).
# Это допустимо ТОЛЬКО если в повторном часе отсутствуют превышения порога.
#
# Если в дубль-часе есть превышения, необходимо:
# 1) использовать timezone-aware datetime (tz_localize),
# 2) либо реализовать корректную логику разделения "первого" и "второго" часа.
#
# Перед использованием скрипта для отчётности обязательно проверить:
# df.index.duplicated().any()
# -------------------------------------------------------------------------

"""
Анализ превышений напряжения (инциденты) + отчёт по качеству данных.

Что делает скрипт:
1) Читает Excel (лист с минутными данными), берёт:
   - timestamp (время измерения)
   - u_kv      (напряжение, кВ)
2) Строит "инциденты" превышения порога (по умолчанию 1.1 * Unom).
3) Сохраняет результаты в новый Excel:
   - incidents              : таблица инцидентов
   - quality_summary        : краткое резюме по качеству данных
   - duplicate_timestamps   : где есть дубли времени (например, из-за перевода часов)
   - missing_minutes        : пропуски по времени (если есть)
   - nan_rows               : строки, где timestamp или u_kv не распарсились (NaN)

Важно про окончание инцидента:
- "Последняя минута инцидента" в исходной логике — это последняя минута, где ещё было превышение.
- Но часто в отчётах удобнее считать окончанием момент, когда превышение закончилось,
  то есть на 1 минуту позже последней минуты превышения.
  Поэтому:
      end_time = last_exceed_time + 1 minute

Если у вас частота не 1 минута — поменяйте FREQ = "1min".
"""

from __future__ import annotations

import pandas as pd


# -------------------------------
# ПАРАМЕТРЫ (настройте под себя)
# -------------------------------
INPUT_XLSX = r"D:\ALEX\ICS\U_1min_2025.xlsx"   # путь к исходному Excel
SHEET_NAME = "10.25"                # имя листа
SKIPROWS = 4                        # сколько строк пропустить вверху (как в вашем файле)

# В вашем файле нужные поля лежат в колонках Unnamed: 2 (время) и Unnamed: 8 (напряжение)
TS_COL = "Unnamed: 2"
U_COL = "Unnamed: 8"

UNOM_KV = 330
THRESHOLD_KV = 1.1 * UNOM_KV        # порог превышения
FREQ = "1min"                       # ожидаемая частота данных (для поиска пропусков)
DEDUP_RULE = "max"                  # как склеивать дубли времени: "max" или "mean"

OUTPUT_XLSX = "incidents_and_quality_report.xlsx"


def load_data(path: str) -> pd.DataFrame:
    """Загрузка и первичная нормализация типов."""
    raw = pd.read_excel(path, sheet_name=SHEET_NAME, skiprows=SKIPROWS, decimal=",")

    df = pd.DataFrame({
        "timestamp": pd.to_datetime(raw[TS_COL], errors="coerce"),
        "u_kv": pd.to_numeric(raw[U_COL], errors="coerce"),
    })

    # Выкидываем строки, где нет времени (такие строки невозможно корректно анализировать)
    df = df.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()
    return df


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Убираем дубли временных меток.

    Почему бывают дубли:
    - перевод времени (DST), выгрузки из SCADA/АСДУ и т.п.
    Если оставить дубли в индексе, некоторые операции/отчёты становятся неоднозначными.

    DEDUP_RULE:
    - "max"  : берём максимум в эту минуту (часто логично для превышений)
    - "mean" : берём среднее
    """
    if df.index.is_unique:
        return df

    if DEDUP_RULE == "mean":
        return df.groupby(level=0).mean(numeric_only=True)
    return df.groupby(level=0).max(numeric_only=True)


def build_incidents(df: pd.DataFrame) -> pd.DataFrame:
    """Поиск и агрегация инцидентов превышения."""
    s = df["u_kv"]

    # above: True там, где напряжение строго выше порога.
    # Если хотите включать ровно пороговое значение — замените ">" на ">=".
    above = s > THRESHOLD_KV

    # starts: старт инцидента — когда стало True, а минуту назад было False
    starts = above & ~above.shift(1, fill_value=False)

    # Каждому старту присваиваем новый номер инцидента (cumsum)
    incident_id = starts.cumsum()

    # Берём только минуты превышения
    exceed = df.loc[above].copy()
    exceed["incident_id"] = incident_id[above]

    incidents = (
        exceed.groupby("incident_id")
        .agg(
            start_time=("u_kv", lambda x: x.index.min()),
            last_exceed_time=("u_kv", lambda x: x.index.max()),
            minutes=("u_kv", "size"),
            max_kv=("u_kv", "max"),
            max_time=("u_kv", lambda x: x.idxmax()),
            mean_kv=("u_kv", "mean"),
        )
        .reset_index()
    )

    # Конец инцидента — на 1 минуту позже последней минуты превышения
    incidents["end_time"] = incidents["last_exceed_time"] + pd.Timedelta(minutes=1)

    # Длительность (в минутах) — от start_time до end_time
    incidents["duration_minutes"] = (
        (incidents["end_time"] - incidents["start_time"]).dt.total_seconds() / 60
    )

    # Удобный порядок колонок
    incidents = incidents[
        [
            "incident_id",
            "start_time",
            "end_time",
            #"last_exceed_time",
            "duration_minutes",
            #"minutes",
            "max_kv",
            "max_time",
            "mean_kv",
        ]
    ]
    return incidents


def build_quality_report(df_original: pd.DataFrame, df_dedup: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Отчёты по качеству: NaN, дубли, пропуски по времени."""
    # NaN (после парсинга)
    nan_rows = df_original[df_original["u_kv"].isna()].reset_index()

    # Дубли временных меток в оригинале (до дедупликации)
    dup_mask = df_original.index.duplicated(keep=False)
    dups = df_original.loc[dup_mask].reset_index()

    dups_agg = (
        dups.groupby("timestamp")["u_kv"]
        .agg(count="size", min_u_kv="min", max_u_kv="max")
        .reset_index()
        .sort_values("timestamp")
    )

    # Пропуски по минутам (после дедупликации)
    full_index = pd.date_range(df_dedup.index.min(), df_dedup.index.max(), freq=FREQ)
    missing = full_index.difference(df_dedup.index)
    missing_df = pd.DataFrame({"missing_timestamp": missing})

    summary = pd.DataFrame([{
        "rows_original": len(df_original),
        "rows_after_dedup": len(df_dedup),
        "time_start": df_dedup.index.min(),
        "time_end": df_dedup.index.max(),
        "nan_u_kv": int(df_original["u_kv"].isna().sum()),
        "duplicate_rows_involved": int(dup_mask.sum()),
        "duplicate_unique_timestamps": int(dups_agg.shape[0]),
        "missing_minutes_after_dedup": int(len(missing)),
        "threshold_kv": THRESHOLD_KV,
        "dedup_rule": DEDUP_RULE,
        "freq_checked": FREQ,
    }])

    return {
        "quality_summary": summary,
        "duplicate_timestamps": dups_agg,
        "missing_minutes": missing_df,
        "nan_rows": nan_rows,
    }


def main() -> None:
    df = load_data(INPUT_XLSX)
        
    # --- Проверка на дубли timestamp ---
    if df.index.duplicated().any():
        print("\033[91m⚠ ВНИМАНИЕ: обнаружены дубли timestamp (возможен переход на зимнее время)\033[0m")
    else:
        print("OK: дубликатов timestamp нет")

    # Чтобы отчёт по дублям был честным, сохраняем копию "как есть"
    df_original = df.copy()

    # Убираем дубли времени (если есть)
    df = deduplicate(df)

    incidents = build_incidents(df)
    quality = build_quality_report(df_original, df)

    # Сохраняем всё в один Excel
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        incidents.to_excel(writer, sheet_name="incidents", index=False)
        for sheet, table in quality.items():
            table.to_excel(writer, sheet_name=sheet, index=False)

    print(f"Готово. Файл сохранён: {OUTPUT_XLSX}")
    print(f"Инцидентов найдено: {len(incidents)}")


if __name__ == "__main__":
    main()