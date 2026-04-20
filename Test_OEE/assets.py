
import pandas as pd
import numpy as np
from pathlib import Path
import datetime

from dagster import asset, get_dagster_logger


# =====================================================
# TP FORMAT HELPERS (3-SHEET: TP_Output / TP_Downtime / TP_Defect)
# =====================================================

def _parse_thai_be_date(series: pd.Series) -> pd.Series:
    """แปลง Date column ที่อาจแสดงปี พ.ศ. (BE) ให้เป็น ค.ศ. (CE)

    กรณีที่รองรับ:
    - Excel serial date → calamine อ่านเป็น Timestamp CE แล้ว (ไม่ต้องแปลง)
    - Timestamp ที่ปี > 2400  → ลบ 543
    - String "1-Sep-68" → pandas parse เป็น 2068 → บวก 500 - 543 = ลบ 43 → 2025
    """
    def _one(val):
        if val is None or (isinstance(val, float) and np.isnan(val)):
            return pd.NaT
        if isinstance(val, (pd.Timestamp, datetime.datetime)):
            dt = pd.Timestamp(val)
            if dt.year > 2400:          # BE year เช่น 2568
                dt = dt.replace(year=dt.year - 543)
            return dt.normalize()
        s = str(val).strip()
        if not s or s.lower() in ("nan", "nat", "none", ""):
            return pd.NaT
        try:
            dt = pd.to_datetime(s, dayfirst=True)
            if dt.year > 2400:          # BE เต็ม เช่น 2568
                dt = dt.replace(year=dt.year - 543)
            elif 2050 <= dt.year <= 2150:
                # 2-digit BE year ที่ pandas อ่านเป็น 20xx
                # เช่น "68" → 2068 แต่หมายถึง พ.ศ. 2568 → ค.ศ. 2025
                dt = dt.replace(year=dt.year + 500 - 543)
            return dt.normalize()
        except Exception:
            return pd.NaT
    return series.apply(_one)


def _load_tp_format(file_path: str, log) -> pd.DataFrame:
    """โหลด 3 sheets TP format แล้ว return DataFrame เดียว
    ที่ compatible กับ oee_rename_clean และ steps ถัดไป

    Row types ใน combined DataFrame:
      - output rows  : มีข้อมูล production (ยอดผลิต, เป้า, เวลาเริ่ม/หยุด)
      - downtime rows: มีข้อมูล downtime (Downtime_Code, เครื่องหยุด, DT start/end)
      - defect rows  : มีข้อมูล defect (Defect_Code, Defect_Pcs)
    """

    # ── TP_Output ──────────────────────────────────────────────────────────────
    out_df = pd.read_excel(file_path, sheet_name="TP_Output", engine="calamine")
    log.info(f"TP_Output  : {len(out_df)} rows | cols: {list(out_df.columns)}")

    out_col_map = {
        "Date":                 "Date",
        "Machine No":           "Machine No",
        "Shift":                "Shift",
        "เวลาเริ่ม":             "เวลาเริ่ม",
        "เวลาหยุดงาน":           "เวลาหยุดงาน",
        "Part No.":             "Part No.",
        "Ref part and step":    "Ref_part_and_step",
        "ยอดผลิต":              "ยอดผลิต",
        "เป้าต่อชั่วโมง(100%)": "เป้าต่อชั่วโมง",
        "ประเภท":               "ประเภท",
    }
    out_df = out_df.rename(columns={k: v for k, v in out_col_map.items() if k in out_df.columns})

    # สร้าง Ref_part_and_step จาก Part No. + Step ถ้ายังไม่มี
    if "Ref_part_and_step" not in out_df.columns and "Part No." in out_df.columns:
        step_s = out_df["Step"].astype(str).str.strip() if "Step" in out_df.columns else ""
        out_df["Ref_part_and_step"] = (
            out_df["Part No."].astype(str).str.strip() + "|" + step_s
        )

    out_df["Defect_Pcs"]              = 0.0
    out_df["Defect_Code"]             = np.nan
    out_df["Downtime_Code"]           = np.nan
    out_df["เครื่องหยุด"]             = np.nan
    out_df["เวลาเครื่องหยุด|เริ่ม"]  = np.nan
    out_df["เวลาเครื่องหยุด|จบ"]     = np.nan
    out_df["source_sheet"]            = "TP_Output"

    # ── TP_Downtime ────────────────────────────────────────────────────────────
    dt_df = pd.read_excel(file_path, sheet_name="TP_Downtime", engine="calamine")
    log.info(f"TP_Downtime: {len(dt_df)} rows | cols: {list(dt_df.columns)}")

    dt_col_map = {
        "Date":           "Date",
        "Machine No":     "Machine No",
        "Shift":          "Shift",
        "เริ่ม":           "เวลาเครื่องหยุด|เริ่ม",
        "จบ":             "เวลาเครื่องหยุด|จบ",
        "เครื่องหยุด":    "เครื่องหยุด",
        "Downtime code":  "Downtime_Code",   # ชื่อปกติ
        "Downtine code":  "Downtime_Code",   # typo variant
        "รายละเอียด":     "รายละเอียด",
        "ประเภท":         "ประเภท",
    }
    dt_df = dt_df.rename(columns={k: v for k, v in dt_col_map.items() if k in dt_df.columns})

    dt_df["ยอดผลิต"]      = 0.0
    dt_df["Defect_Pcs"]   = 0.0
    dt_df["Defect_Code"]  = np.nan
    dt_df["เป้าต่อชั่วโมง"] = 0.0
    dt_df["เวลาเริ่ม"]    = np.nan
    dt_df["เวลาหยุดงาน"]  = np.nan
    dt_df["source_sheet"] = "TP_Downtime"

    # ── TP_Defect ──────────────────────────────────────────────────────────────
    def_df = pd.read_excel(file_path, sheet_name="TP_Defect", engine="calamine")
    log.info(f"TP_Defect  : {len(def_df)} rows | cols: {list(def_df.columns)}")

    # TP_Defect มี 2 columns สำหรับ ref: "Ref part and step" (Part No.) + column ถัดไป (Step)
    # pandas อาจ rename dup column เป็น "Ref part and step.1" หรือ "Ref part and step.2"
    def_col_map = {
        "Date":        "Date",
        "Machine No":  "Machine No",
        "Shift":       "Shift",
        "Defect Code": "Defect_Code",
        "Defect(Pcs)": "Defect_Pcs",
        "ประเภท":      "ประเภท",
    }
    def_df = def_df.rename(columns={k: v for k, v in def_col_map.items() if k in def_df.columns})

    # TP_Defect มี 2 columns ที่ชื่อ "Ref part and step" (Part No. และ Step)
    # pandas จะ auto-deduplicate เป็น "Ref part and step", "Ref part and step.1" (หรือ .2)
    # ใช้ positional search แทนการ rename โดยชื่อ เพื่อหลีกเลี่ยง duplicate column name
    ref_cols = [c for c in def_df.columns if str(c).startswith("Ref part and step")]
    if len(ref_cols) >= 1:
        def_df = def_df.rename(columns={ref_cols[0]: "_ref_part"})
    if len(ref_cols) >= 2:
        def_df = def_df.rename(columns={ref_cols[1]: "_ref_step"})

    if "_ref_part" in def_df.columns:
        step_s = def_df["_ref_step"].astype(str).str.strip() if "_ref_step" in def_df.columns else ""
        def_df["Ref_part_and_step"] = (
            def_df["_ref_part"].astype(str).str.strip() + "|" + step_s
        )
        def_df["Part No."] = def_df["_ref_part"]
        def_df.drop(columns=[c for c in ("_ref_part", "_ref_step") if c in def_df.columns], inplace=True)

    # ไม่ใช้ ยอดผลิต / เป้าต่อชั่วโมง จาก TP_Defect เพื่อไม่ให้นับซ้ำกับ TP_Output
    def_df["ยอดผลิต"]              = 0.0
    def_df["เป้าต่อชั่วโมง"]       = 0.0
    def_df["Downtime_Code"]         = np.nan
    def_df["เครื่องหยุด"]           = np.nan
    def_df["เวลาเริ่ม"]             = np.nan
    def_df["เวลาหยุดงาน"]           = np.nan
    def_df["เวลาเครื่องหยุด|เริ่ม"] = np.nan
    def_df["เวลาเครื่องหยุด|จบ"]   = np.nan
    def_df["source_sheet"]          = "TP_Defect"

    # ── Parse dates ────────────────────────────────────────────────────────────
    for df in (out_df, dt_df, def_df):
        if "Date" in df.columns:
            df["Date"] = _parse_thai_be_date(df["Date"])

    # ── Normalize Shift: "6 A" → "A"  (ถ้า Shift column มี machine + shift รวมกัน) ──
    for df in (out_df, dt_df, def_df):
        if "Shift" in df.columns:
            df["Shift"] = (
                df["Shift"].astype(str).str.strip()
                .str.split().str[-1]   # เอา word สุดท้าย เช่น "6 A" → "A"
                .str.upper()
            )

    # ── ตรวจสอบ duplicate columns ก่อน concat (ป้องกัน InvalidIndexError) ──────
    def _dedup_cols(df: pd.DataFrame) -> pd.DataFrame:
        cols = pd.Series(df.columns)
        for dup in cols[cols.duplicated()].unique():
            idxs = cols[cols == dup].index.tolist()
            for rank, i in enumerate(idxs):
                if rank > 0:
                    cols.iloc[i] = f"{dup}_{rank}"
        df.columns = cols
        return df

    out_df = _dedup_cols(out_df)
    dt_df  = _dedup_cols(dt_df)
    def_df = _dedup_cols(def_df)

    # ── Combine ────────────────────────────────────────────────────────────────
    combined = pd.concat([out_df, dt_df, def_df], ignore_index=True)
    combined["source_file"] = Path(file_path).name

    # ── ป้องกัน duplicate columns เมื่อ oee_rename_clean รัน rename_map ──────
    # ถ้า column "ต้นทาง" (key) และ "ปลายทาง" (value) มีอยู่พร้อมกัน
    # → drop ต้นทางออก เพราะ loader ได้ set ค่าที่ถูกต้องใน column ปลายทางแล้ว
    _conflict_map = {
        "จำนวน":                "Defect_Pcs",
        "Defect(Pcs)":          "Defect_Pcs",
        "Defect Code":          "Defect_Code",
        "เป้าต่อชั่วโมง(100%)": "เป้าต่อชั่วโมง",
        "Downtine code":        "Downtime_Code",
        "Downtime code":        "Downtime_Code",
    }
    drop_cols = [
        k for k, v in _conflict_map.items()
        if k in combined.columns and v in combined.columns
    ]
    if drop_cols:
        combined = combined.drop(columns=drop_cols)

    return combined


# =====================================================
# STEP 1 : LOAD FILES & FIX MERGED HEADERS
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def oee_load_raw() -> pd.DataFrame:
    root_path = r"C:\Users\instulno\OneDrive - MATTEL INC\Desktop\Find Error OEE\example data2"
    debug = False
    log = get_dagster_logger()

    root = Path(root_path)
    if not root.exists():
        raise ValueError(f"Root folder not found: {root}")

    # ── ตรวจสอบ TP format (ไฟล์เดี่ยวที่มี 3 sheets) ─────────────────────────
    if root.is_file() and root.suffix.lower() in (".xlsx", ".xlsm"):
        xls = pd.ExcelFile(root, engine="calamine")
        tp_required = {"TP_Output", "TP_Downtime", "TP_Defect"}
        if tp_required.issubset(set(xls.sheet_names)):
            log.info("🗂️  TP format detected — loading TP_Output / TP_Downtime / TP_Defect")
            return _load_tp_format(str(root), log)

    # ── Old format: scan folder for xlsx files ────────────────────────────────
    if root.is_file():
        # ไฟล์เดี่ยวที่ไม่ใช่ TP format → scan parent folder
        root = root.parent

    if debug:
        log.info(f"📂 Start processing: {root}")

    all_df = []
    excel_files = list(root.rglob("*.xlsx"))

    for file in excel_files:
        if file.name.startswith("~$"):
            continue
        try:
            xls = pd.ExcelFile(file, engine="calamine")
            for sheet in xls.sheet_names:
                if "คู่มือ" in sheet:
                    continue

                temp_df = pd.read_excel(file, sheet_name=sheet, header=None, engine="calamine")

                mask = temp_df.astype(str).apply(
                    lambda x: x.str.contains("Machine No", case=False, na=False)
                ).any(axis=1)
                if not mask.any():
                    continue
                h_idx = mask.idxmax()

                if h_idx > 0:
                    top_s = temp_df.iloc[h_idx - 1].copy()
                    top_s = top_s.mask(top_s.astype(str).str.strip() == "")
                    top_s = top_s.mask(top_s.astype(str).str.lower() == "nan")
                    top_s = top_s.ffill().fillna("").astype(str).str.strip()
                else:
                    top_s = pd.Series([""] * temp_df.shape[1])

                bot_h = temp_df.iloc[h_idx].astype(str).str.strip()

                new_cols = []
                dt_subcols = [
                    "เริ่ม", "จบ", "จำนวนครั้ง", "จำนวน", "ครั้ง",
                    "เวลาเครื่องหยุด", "เวลาหยุดเครื่อง", "นาที",
                ]

                for t, b in zip(top_s, bot_h):
                    if b.lower() == "nan":
                        b = ""
                    if t.lower() == "nan":
                        t = ""
                    if b in dt_subcols and t != "":
                        new_cols.append(f"{t}|{b}")
                    elif b != "":
                        new_cols.append(b)
                    elif t != "":
                        new_cols.append(t)
                    else:
                        new_cols.append("Unnamed")

                temp_df.columns = new_cols
                temp_df = temp_df.iloc[h_idx + 1:].reset_index(drop=True)
                temp_df = temp_df.loc[:, ~temp_df.columns.str.contains("^Unnamed|^$")]
                temp_df = temp_df.loc[:, ~temp_df.columns.duplicated(keep="first")]

                temp_df["source_file"] = file.name
                temp_df["source_sheet"] = sheet

                all_df.append(temp_df)
        except Exception as e:
            log.error(f"❌ Cannot read {file.name}: {e}")

    if not all_df:
        raise ValueError("ไม่พบข้อมูลที่ใช้งานได้")

    df = pd.concat(all_df, ignore_index=True)
    return df


# =====================================================
# STEP 2 : RENAME & CLEAN
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def oee_rename_clean(oee_load_raw: pd.DataFrame) -> pd.DataFrame:
    df = oee_load_raw.copy()

    rename_map = {
        "Machine No": "Machine No",
        "Shift": "Shift",
        "Date": "Date",
        "ยอดผลิต": "ยอดผลิต",
        "จำนวน": "Defect_Pcs",
        "Defect(Pcs)": "Defect_Pcs",
        "Defect Code": "Defect_Code",
        "เริ่ม": "เวลาเริ่ม",
        "จบ": "เวลาหยุดงาน",
        "เป้าต่อชั่วโมง(100%)": "เป้าต่อชั่วโมง",
        "Part No.": "Part No.",
        "Ref part and step": "Ref_part_and_step",
        "รายละเอียด": "รายละเอียด",
        "จำนวนครั้ง": "จำนวนครั้ง",
        "เครื่องหยุด": "เครื่องหยุด",
        "Downtine code": "Downtime_Code",
    }
    df = df.rename(columns=rename_map)

    required_cols = [
        "Machine No", "Shift", "Date", "เวลาเริ่ม", "เวลาหยุดงาน",
        "ยอดผลิต", "Defect_Pcs", "เป้าต่อชั่วโมง", "Part No.",
        "Ref_part_and_step", "Defect_Code", "Downtime_Code",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    num_cols = ["ยอดผลิต", "Defect_Pcs", "เป้าต่อชั่วโมง"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["Shift"] = df["Shift"].astype(str).str.strip().str.upper()
    df["Shift"] = df["Shift"].where(df["Shift"].isin(["A", "B"]), other=np.nan)

    # Step 1: ffill Shift จากแถว ยอดยกมา ลงมา (within Machine No group)
    # แถว ยอดยกมา มี Shift=A/B → แถว output/DT ด้านล่างที่ว่างจะรับค่าต่อ
    temp_machine = df["Machine No"].ffill()
    df["Shift"] = df.groupby(temp_machine, sort=False)["Shift"].ffill()

    # Step 2: fallback — infer จาก เวลาเริ่ม ถ้ายังว่างอยู่
    # Shift A: เริ่ม 06:00–13:59  |  Shift B: เริ่ม 14:00–05:59
    mask_missing = ~df["Shift"].isin(["A", "B"])
    if mask_missing.any() and "เวลาเริ่ม" in df.columns:
        def _infer_shift(val):
            if pd.isna(val):
                return np.nan
            try:
                if isinstance(val, datetime.time):
                    h = val.hour
                elif isinstance(val, (pd.Timestamp, datetime.datetime)):
                    h = pd.Timestamp(val).hour
                elif isinstance(val, (pd.Timedelta, datetime.timedelta)):
                    h = int(val.total_seconds() // 3600) % 24
                else:
                    td = pd.to_timedelta(str(val).split()[-1], errors="coerce")
                    if pd.isna(td):
                        return np.nan
                    h = int(td.total_seconds() // 3600) % 24
                return "A" if 6 <= h < 14 else "B"
            except Exception:
                return np.nan
        inferred = df.loc[mask_missing, "เวลาเริ่ม"].apply(_infer_shift)
        df.loc[mask_missing, "Shift"] = inferred

    df = df[df["Shift"].isin(["A", "B"])].copy()
    df["Machine No"] = df["Machine No"].ffill()

    if np.issubdtype(df["Date"].dtype, np.number):
        df["Date"] = pd.to_datetime(
            df["Date"], origin="1899-12-30", unit="D"
        ).dt.normalize()
    else:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.normalize()
    df = df.dropna(subset=["Date"])
    df = df[df["Date"].dt.year >= 2000].copy()

    df["Part No."] = df["Part No."].astype(str).str.strip().str.upper()
    df["Part No."] = df["Part No."].replace(
        to_replace=[r"^\s*$", "^NAN$", "^NONE$"], value=np.nan, regex=True
    )

    df["Downtime_Code"] = df["Downtime_Code"].astype(str).str.strip().str.upper()
    df["Downtime_Code"] = df["Downtime_Code"].replace(
        to_replace=[r"^\s*$", "^NAN$", "^NONE$"], value=np.nan, regex=True
    )

    # =========================================================
    # FIX: ลบ truly-empty template rows ก่อน step ถัดไป
    # เงื่อนไข: ไม่มีข้อมูลจริงใดๆ เลย (ไม่มี output, defect, DT code, duration)
    # rows แบบนี้คือ template slots ที่ไม่ได้กรอก
    # =========================================================
    def _to_min_series(series):
        """แปลง duration column เป็น minutes (vectorized)"""
        def _single(val):
            if val is None:
                return 0.0
            if isinstance(val, float) and np.isnan(val):
                return 0.0
            if isinstance(val, pd.Timedelta):
                return val.total_seconds() / 60
            if isinstance(val, datetime.timedelta):
                return val.total_seconds() / 60
            if isinstance(val, datetime.time):
                return val.hour * 60 + val.minute + val.second / 60
            s = str(val).strip()
            if s in ("", "nan", "None", "NaN"):
                return 0.0
            n = pd.to_numeric(s, errors="coerce")
            if pd.notna(n):
                if 0 < n <= 1:
                    return n * 24 * 60
                if n > 1:
                    return float(n)
            try:
                return pd.to_timedelta(s.split()[-1]).total_seconds() / 60
            except Exception:
                return 0.0
        return series.apply(_single)

    dur_min_raw = (
        _to_min_series(df["เครื่องหยุด"]) if "เครื่องหยุด" in df.columns
        else pd.Series(0.0, index=df.index)
    )

    is_no_output  = df["ยอดผลิต"] <= 0
    is_no_defect  = df["Defect_Pcs"] <= 0
    is_no_dt_code = df["Downtime_Code"].isna()
    is_no_dur     = dur_min_raw == 0.0

    # เวลาเริ่ม = '00:00' (string) คือ template slot ว่าง
    start_str = df["เวลาเริ่ม"].astype(str).str.strip()
    is_empty_start = start_str.isin(["00:00", "", "nan", "None", "NaT"])

    is_truly_empty = (
        is_no_output
        & is_no_defect
        & is_no_dt_code
        & is_no_dur
        & is_empty_start
    )

    df = df[~is_truly_empty].reset_index(drop=True)

    return df


# =====================================================
# STEP 3 : DOWNTIME CALCULATION
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def oee_with_downtime(oee_rename_clean: pd.DataFrame) -> pd.DataFrame:
    df = oee_rename_clean.copy()

    def parse_minutes_vec(series):
        s = series.copy()
        mask_td = s.apply(lambda x: isinstance(x, (pd.Timedelta, datetime.timedelta)))
        out = pd.Series(0.0, index=s.index)
        out[mask_td] = s[mask_td].apply(
            lambda x: x.total_seconds() / 60
            if isinstance(x, (pd.Timedelta, datetime.timedelta))
            else 0.0
        )

        mask_dt = s.apply(
            lambda x: isinstance(x, (pd.Timestamp, datetime.datetime, datetime.time))
        )
        out[mask_dt] = s[mask_dt].apply(
            lambda x: (
                pd.Timestamp(x).hour * 60
                + pd.Timestamp(x).minute
                + pd.Timestamp(x).second / 60
            )
            if isinstance(x, (pd.Timestamp, datetime.datetime))
            else (x.hour * 60 + x.minute + x.second / 60)
        )

        remaining = ~mask_td & ~mask_dt
        numeric = pd.to_numeric(s[remaining], errors="coerce")
        mask_frac = (numeric > 0) & (numeric <= 1)
        mask_min  = (numeric > 1) & (numeric <= 1440)
        out[remaining & mask_frac.reindex(s.index, fill_value=False)] = (
            numeric[mask_frac] * 24 * 60
        )
        out[remaining & mask_min.reindex(s.index, fill_value=False)] = numeric[mask_min]

        str_mask = remaining & s.astype(str).str.contains(":", na=False)
        out[str_mask] = s[str_mask].apply(
            lambda x: (
                pd.to_timedelta(str(x).strip().split()[-1], errors="coerce").total_seconds() / 60
                if pd.notna(
                    pd.to_timedelta(str(x).strip().split()[-1], errors="coerce")
                )
                else 0.0
            )
        )
        return out.fillna(0.0).clip(lower=0, upper=1440)

    col_dur = (
        df["เครื่องหยุด"]
        if "เครื่องหยุด" in df.columns
        else pd.Series(np.nan, index=df.index)
    )
    dur_min = parse_minutes_vec(col_dur)

    DT_START_COL, DT_END_COL = None, None
    for c in df.columns:
        if "เวลาเครื่องหยุด" in c and "เริ่ม" in c:
            DT_START_COL = c
        if "เวลาเครื่องหยุด" in c and "จบ" in c:
            DT_END_COL = c

    def _to_min(t):
        if pd.isna(t):
            return np.nan
        if isinstance(t, datetime.time):
            return t.hour * 60 + t.minute + t.second / 60
        if isinstance(t, (pd.Timedelta, datetime.timedelta)):
            return t.total_seconds() / 60
        if isinstance(t, (pd.Timestamp, datetime.datetime)):
            return t.hour * 60 + t.minute + t.second / 60
        return np.nan

    MB_S, MB_E = 11 * 60 + 30, 12 * 60 + 20
    OB_S, OB_E = 16 * 60 + 50, 17 * 60 + 20
    N1_S, N1_E = 0 * 60 + 0,   0 * 60 + 30
    N2_S, N2_E = 3 * 60 + 0,   3 * 60 + 10
    N3_S, N3_E = 6 * 60 + 0,   6 * 60 + 30

    def overlap(s, e, bs, be):
        return np.maximum(0, np.minimum(e, be) - np.maximum(s, bs))

    if DT_START_COL and DT_END_COL:
        dt_start_min = df[DT_START_COL].apply(_to_min).values
        dt_end_min   = df[DT_END_COL].apply(_to_min).values

        # FIX: dt_end = 0 (00:00) ที่เป็น template placeholder → ถือว่าไม่มีข้อมูล
        # ถ้า start > 0 แต่ end == 0 → end = NaN (ไม่คำนวณ overlap)
        dt_end_min = np.where(
            (dt_start_min > 0) & (dt_end_min == 0),
            np.nan,
            dt_end_min,
        )

        dt_end_adj = np.where(
            (dt_end_min < dt_start_min)
            & np.isfinite(dt_start_min)
            & np.isfinite(dt_end_min),
            dt_end_min + 1440,
            dt_end_min,
        )
        dt_break_overlap = (
            overlap(dt_start_min, dt_end_adj, MB_S, MB_E)
            + overlap(dt_start_min, dt_end_adj, OB_S, OB_E)
            + overlap(dt_start_min, dt_end_adj, N1_S + 1440, N1_E + 1440)
            + overlap(dt_start_min, dt_end_adj, N2_S + 1440, N2_E + 1440)
            + overlap(dt_start_min, dt_end_adj, N3_S + 1440, N3_E + 1440)
        )
        dt_break_overlap = np.nan_to_num(dt_break_overlap, nan=0.0)
    else:
        dt_break_overlap = np.zeros(len(df))

    dur_min_adj = np.maximum(dur_min - dt_break_overlap, 0.0)

    code = df["Downtime_Code"]
    NOPLAN_CODES  = ["F01", "F10"]
    PM_CODES      = ["F11", "F13", "F18"]
    EXCLUDE_CODES = ["F15"]

    is_noplan    = code.isin(NOPLAN_CODES)
    is_pm        = code.isin(PM_CODES)
    is_exclude   = code.isin(EXCLUDE_CODES)
    has_code     = code.notna()
    is_breakdown = has_code & ~is_noplan & ~is_pm & ~is_exclude

    # NoPlan: คำนวณ duration จาก start/end times ตาม PBI DAX
    # เมื่อ end=00:00 และ start>0 → dt_end_min ถูก NaN ไปแล้ว → dur=0 (ตรงกับ PBI overlap=0)
    if DT_START_COL and DT_END_COL:
        noplan_dur = np.where(
            np.isfinite(dt_start_min) & np.isfinite(dt_end_adj),
            np.maximum(dt_end_adj - dt_start_min - dt_break_overlap, 0.0),
            0.0,
        )
    else:
        noplan_dur = dur_min_adj

    df["dt_noplan"]    = np.where(is_noplan,    noplan_dur,  0.0)
    df["dt_pm"]        = np.where(is_pm,        dur_min_adj, 0.0)
    df["dt_breakdown"] = np.where(is_breakdown, dur_min_adj, 0.0)
    df["dt_exclude"]   = np.where(is_exclude,   dur_min_adj, 0.0)

    raw_start = df["เวลาเริ่ม"].apply(_to_min)
    raw_end   = df["เวลาหยุดงาน"].apply(_to_min)
    adj_end   = np.where(
        raw_start > raw_end, raw_end + 1440, raw_end
    )

    s = raw_start.values
    e = adj_end

    break_min = (
        overlap(s, e, MB_S, MB_E)
        + overlap(s, e, OB_S, OB_E)
        + overlap(s, e, N1_S + 1440, N1_E + 1440)
        + overlap(s, e, N2_S + 1440, N2_E + 1440)
        + overlap(s, e, N3_S + 1440, N3_E + 1440)
    )
    is_prod = df["ยอดผลิต"] > 0
    df["Break_Time_Min"] = np.where(
        is_prod & ~(pd.isna(raw_start) | pd.isna(raw_end)),
        break_min,
        0.0,
    )

    df["PM_Break_Min"]       = df["dt_pm"]
    df["Breakdown_Min"]      = df["dt_breakdown"]
    df["No_Plan_DT_Min"]     = df["dt_noplan"]
    df["Total_Downtime_Min"] = df["PM_Break_Min"] + df["Breakdown_Min"]

    return df


# =====================================================
# STEP 4 : TIME LOGIC
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def oee_with_time_logic(oee_with_downtime: pd.DataFrame) -> pd.DataFrame:
    df = oee_with_downtime.copy()

    def safe_convert_time(col_name):
        target_col = df[col_name]
        if isinstance(target_col, pd.DataFrame):
            target_col = target_col.iloc[:, 0]

        def parse_single(val):
            # กรอง template placeholder ออก
            if val is None:
                return pd.NaT
            if isinstance(val, float) and np.isnan(val):
                return pd.NaT
            s = str(val).strip()
            if s in ("", "00:00", "nan", "None", "NaT", "ยอดยกมา"):
                return pd.NaT

            if isinstance(val, pd.Timedelta):
                return val
            if isinstance(val, datetime.timedelta):
                return pd.Timedelta(seconds=val.total_seconds())
            if isinstance(val, (pd.Timestamp, datetime.datetime)):
                t = pd.Timestamp(val)
                return pd.Timedelta(hours=t.hour, minutes=t.minute, seconds=t.second)
            if isinstance(val, datetime.time):
                return pd.Timedelta(
                    hours=val.hour, minutes=val.minute, seconds=val.second
                )

            num = pd.to_numeric(s, errors="coerce")
            if pd.notna(num):
                return pd.to_timedelta(num, unit="D")

            td = pd.to_timedelta(s.split()[-1], errors="coerce")
            if pd.notna(td):
                return td

            dt = pd.to_datetime(s, errors="coerce")
            if pd.notna(dt):
                return pd.Timedelta(
                    hours=dt.hour, minutes=dt.minute, seconds=dt.second
                )

            return pd.NaT

        return target_col.apply(parse_single)

    if "เวลาเริ่ม" in df.columns and "เวลาหยุดงาน" in df.columns:
        df["td_start"] = safe_convert_time("เวลาเริ่ม")
        df["td_end"]   = safe_convert_time("เวลาหยุดงาน")

        # ต้องมีทั้ง start และ end จึงถือว่าเป็น production/DT row ที่มีเวลา
        has_time = df["td_start"].notna() & df["td_end"].notna()
        df_with_time = df[has_time].copy()
        df_no_time   = df[~has_time].copy()

        df_with_time["start_dt"] = df_with_time["Date"] + df_with_time["td_start"]
        df_with_time["end_dt"]   = df_with_time["Date"] + df_with_time["td_end"]

        mask_cross = df_with_time["end_dt"] < df_with_time["start_dt"]
        df_with_time.loc[mask_cross, "end_dt"] += pd.Timedelta(days=1)

        def fix_continuity(group):
            group = group.copy().reset_index(drop=True)
            for i in range(1, len(group)):
                curr_start   = group.loc[i, "start_dt"]
                prev_start   = group.loc[i - 1, "start_dt"]
                max_end_so_far = group.loc[:i - 1, "end_dt"].max()

                if curr_start == prev_start:
                    continue
                if curr_start < max_end_so_far:
                    days_diff   = int(
                        (max_end_so_far.normalize() - curr_start.normalize()).days
                    )
                    days_to_add = days_diff if days_diff > 0 else 1
                    group.loc[i, "start_dt"] += pd.Timedelta(days=days_to_add)
                    group.loc[i, "end_dt"]   += pd.Timedelta(days=days_to_add)
            return group

        df_with_time = df_with_time.groupby(
            ["Date", "Machine No", "Shift"], group_keys=False
        ).apply(fix_continuity)
        df_with_time = df_with_time.sort_values(
            ["Date", "Machine No", "Shift", "start_dt"]
        ).reset_index(drop=True)

        df_no_time["start_dt"] = pd.NaT
        df_no_time["end_dt"]   = pd.NaT

        df = pd.concat([df_with_time, df_no_time], ignore_index=True)
        df = df.sort_values(["Date", "Machine No", "Shift"]).reset_index(drop=True)

    else:
        df["start_dt"] = pd.NaT
        df["end_dt"]   = pd.NaT

    return df


# =====================================================
# STEP 5 : AGGREGATION
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def oee_shift_summary(oee_with_time_logic: pd.DataFrame) -> pd.DataFrame:
    df = oee_with_time_logic.copy()

    def join_unique(x):
        items = [
            str(i).strip()
            for i in x.dropna().unique()
            if str(i).strip() not in ("", "nan", "None")
        ]
        return ", ".join(sorted(items))

    shift_df = (
        df.groupby(["Date", "Machine No", "Shift"], as_index=False)
        .agg(
            shift_start        = ("start_dt",         "min"),
            shift_end          = ("end_dt",            "max"),
            total_part_count   = ("Part No.",          "nunique"),
            Part_List          = ("Part No.",          join_unique),
            Defect_Code_List   = ("Defect_Code",       join_unique),
            Ref_List           = ("Ref_part_and_step", join_unique),
            Total_Output       = ("ยอดผลิต",           "sum"),
            Total_Defect_Pcs   = ("Defect_Pcs",        "sum"),
            PM_Break_Min       = ("PM_Break_Min",      "sum"),
            Break_Time_Min     = ("Break_Time_Min",    "sum"),
            Breakdown_Min      = ("Breakdown_Min",     "sum"),
            Total_Downtime_Min = ("Total_Downtime_Min","sum"),
            No_Plan_DT_Min     = ("No_Plan_DT_Min",    "sum"),
            # FIX: ใช้ max เฉพาะค่า > 0 เท่านั้น เพื่อหลีกเลี่ยง target=0 จาก ยอดยกมา row
            Target_Per_Hr      = ("เป้าต่อชั่วโมง",    lambda x: x[x > 0].max() if (x > 0).any() else 0),
        )
    )

    shift_df["No_Plan_Min"] = shift_df["No_Plan_DT_Min"]
    return shift_df


# =====================================================
# STEP 6 : PREPARE COLUMNS & GHOST FILTER (FIXED)
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def oee_with_calculations(oee_shift_summary: pd.DataFrame) -> pd.DataFrame:
    shift_df = oee_shift_summary.copy()

    shift_df["Plan_time"] = (
        (shift_df["shift_end"] - shift_df["shift_start"]).dt.total_seconds() / 60
        - shift_df["Break_Time_Min"]
    ).clip(lower=0).round(2)

    valid_target = shift_df["Target_Per_Hr"] > 0

    shift_df["Output(min)"] = 0.0
    shift_df.loc[valid_target, "Output(min)"] = (
        60 * shift_df["Total_Output"] / shift_df["Target_Per_Hr"]
    )

    shift_df["Defect(min)"] = 0.0
    shift_df.loc[valid_target, "Defect(min)"] = (
        60 * shift_df["Total_Defect_Pcs"] / shift_df["Target_Per_Hr"]
    )

    shift_df["ยอดผลิต"]    = shift_df["Total_Output"]
    shift_df["Defect_Pcs"] = shift_df["Total_Defect_Pcs"]

    # Cap No_Plan_Min ไม่ให้เกิน Plan_time (ป้องกัน No_Plan_Min > Plan_time เช่น 780 > 680)
    shift_df["No_Plan_Min"] = np.minimum(shift_df["No_Plan_Min"], shift_df["Plan_time"])

    shift_df["Net_Plantime"] = (
        shift_df["Plan_time"] - shift_df["No_Plan_Min"]
    ).clip(lower=0)

    shift_df["Downtime"]          = shift_df["PM_Break_Min"] + shift_df["Breakdown_Min"]
    shift_df["Total_Downtime_Min"] = shift_df["Downtime"]

    shift_df["Working_time_min"] = (
        shift_df["Net_Plantime"] - shift_df["Downtime"]
    ).clip(lower=0)

    # =========================================================
    # FIX: Ghost filter ที่ถูกต้อง — ลบเฉพาะกะที่ไม่มีข้อมูลจริงใดๆ เลย
    # เดิม: ลบทุก row ที่ไม่มี Part + ไม่มี output → ลบ DT rows ทั้งหมดไปด้วย (ผิด)
    # ใหม่: เก็บกะที่มีอย่างน้อย 1 อย่าง = output OR downtime OR no-plan OR PM
    # =========================================================
    has_real_data = (
        (shift_df["Total_Output"]       > 0)
        | (shift_df["Breakdown_Min"]    > 0)
        | (shift_df["PM_Break_Min"]     > 0)
        | (shift_df["No_Plan_DT_Min"]   > 0)
        | (shift_df["Total_Downtime_Min"] > 0)
    )

    n_before = len(shift_df)
    shift_df = shift_df[has_real_data].reset_index(drop=True)
    n_after  = len(shift_df)

    return shift_df


# =====================================================
# STEP 7 : DATA ACCURACY ENGINE
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def oee_accuracy(oee_with_calculations: pd.DataFrame) -> pd.DataFrame:
    log = get_dagster_logger()
    shift_df = oee_with_calculations.copy()

    def data_accuracy_engine(row):
        errors   = []
        warnings = []

        # ── NO-PLAN SHIFT ───────────────────────────────────────────────────────
        # กะที่ไม่มีแผนผลิต (No_Plan_Min เต็ม หรือ PM/หยุดสายทั้งกะ)
        # ลักษณะ: Net_Plantime=0 → Working_time_min=0, Total_Output=0
        # → Healthy Data เสมอ ไม่ต้องตรวจ rule ใด
        if row["Net_Plantime"] == 0 and row["Working_time_min"] == 0 and row["ยอดผลิต"] == 0:
            return pd.Series(["Healthy Data", "", 0, 0])

        # ── LOGIC GROUP 1 : DEFECT ──────────────────────────────────────────────
        if row["Defect_Pcs"] > row["ยอดผลิต"] and row["ยอดผลิต"] > 0:
            errors.append("DEFECT_GT_OUTPUT")

        # ── LOGIC GROUP 2 : TIME ────────────────────────────────────────────────
        if row["Working_time_min"] <= 0 and row["ยอดผลิต"] > 0:
            errors.append("GHOST_PRODUCTION")

        if row["Working_time_min"] < 0 or row["Downtime"] < 0:
            errors.append("NEGATIVE_TIME")

        # ── LOGIC GROUP 3 : PERFORMANCE ─────────────────────────────────────────
        output_min   = row["Output(min)"]
        working_time = row["Working_time_min"]

        if pd.isna(output_min) or pd.isna(working_time) or working_time <= 0:
            # กะที่มี output แต่ไม่มี working time = error จริง
            if row["ยอดผลิต"] > 0:
                errors.append("INVALID_PERFORMANCE_BASE")
        else:
            perf_ratio = output_min / working_time
            if perf_ratio >= 1.5:
                errors.append("PERFORMANCE_OVER_150PCT")
            elif 1.2 <= perf_ratio < 1.5:
                warnings.append("PERFORMANCE_OVER_120PCT")

        # ── LOGIC GROUP 4 : NO OUTPUT ───────────────────────────────────────────
        if (
            row["Working_time_min"] > 0
            and row["ยอดผลิต"] == 0
            and row["No_Plan_Min"] == 0
            and row["Downtime"] == 0
        ):
            warnings.append("NO_OUTPUT_WITH_TIME")

        # ── FINAL ───────────────────────────────────────────────────────────────
        if errors:
            warnings.clear()
            status = "Error"
            detail = ", ".join(errors)
        elif warnings:
            status = "Warning"
            detail = ", ".join(warnings)
        else:
            status = "Healthy Data"
            detail = ""

        return pd.Series([status, detail, len(errors), len(warnings)])

    shift_df[
        ["Data_Accuracy_Status", "Issue_List", "Error_Count", "Warning_Count"]
    ] = shift_df.apply(data_accuracy_engine, axis=1)

    eval_df   = shift_df.copy()
    eval_rows = len(eval_df)

    issue_df = eval_df[
        eval_df["Issue_List"].notna() & (eval_df["Issue_List"] != "")
    ].copy()
    if not issue_df.empty:
        issue_df = issue_df.copy()
        issue_df["Issue"] = issue_df["Issue_List"].str.split(", ")
        issue_df = issue_df.explode("Issue")
        issue_counts = issue_df["Issue"].value_counts()
        log.info("=" * 80)
        log.info("ERROR DETAIL BREAKDOWN (%)")
        log.info("=" * 80)
        for issue, count in issue_counts.items():
            pct = (count / eval_rows) * 100
            bar = "█" * int(pct / 2)
            log.info(f"{issue:30} : {pct:6.2f}% {bar}")

    status_counts = eval_df["Data_Accuracy_Status"].value_counts()
    healthy = status_counts.get("Healthy Data", 0)
    warning = status_counts.get("Warning", 0)
    error   = status_counts.get("Error", 0)
    healthy_pct = healthy / eval_rows * 100 if eval_rows else 0
    warning_pct = warning / eval_rows * 100 if eval_rows else 0
    error_pct   = error   / eval_rows * 100 if eval_rows else 0

    log.info("=" * 80)
    log.info("BASELINE vs ERROR (%)")
    log.info("=" * 80)
    log.info(f"Evaluated rows : {eval_rows:,}")
    log.info(f"Healthy Data   : {healthy_pct:6.2f}%  ({healthy:,} rows)  {'█' * int(healthy_pct / 2)}")
    log.info(f"Warning        : {warning_pct:6.2f}%  ({warning:,} rows)  {'█' * int(warning_pct / 2)}")
    log.info(f"Error          : {error_pct:6.2f}%  ({error:,} rows)  {'█' * int(error_pct / 2)}")

    return shift_df


# =====================================================
# STEP 8 : EXPORT
# =====================================================

@asset(group_name="Catch_Error_TAMPO")
def export_oee(oee_with_time_logic: pd.DataFrame, oee_accuracy: pd.DataFrame) -> str:
    log = get_dagster_logger()

    output_folder = Path(
        r"C:\Users\instulno\OneDrive - MATTEL INC\Desktop\Find Error OEE"
    )
    output_folder.mkdir(parents=True, exist_ok=True)
    output_file = output_folder / "cleaned_oee_with_accuracy.xlsx"

    healthy_df = oee_accuracy[oee_accuracy["Data_Accuracy_Status"] == "Healthy Data"]
    warning_df = oee_accuracy[oee_accuracy["Data_Accuracy_Status"] == "Warning"]
    error_df   = oee_accuracy[oee_accuracy["Data_Accuracy_Status"] == "Error"]

    drop_raw_cols     = ["td_start", "td_end"]
    drop_summary_cols = [
        "Total_Calendar_Min", "A_Available_Min", "B_Performance_Min",
        "Low_Speed_Min", "C_Quality_Min", "D_Net_Min",
    ]

    raw_export = oee_with_time_logic.drop(
        columns=[c for c in drop_raw_cols if c in oee_with_time_logic.columns]
    )
    raw_export = raw_export.sort_values(
        ["Date", "Machine No", "Shift", "start_dt"]
    ).reset_index(drop=True)

    SUMMARY_COL_ORDER = [
        "Date", "Machine No", "Shift", "shift_start", "shift_end",
        "total_part_count", "Part_List", "Ref_List",
        "Plan_time", "No_Plan_Min", "Net_Plantime", "Break_Time_Min",
        "PM_Break_Min", "Breakdown_Min", "Downtime", "Total_Downtime_Min",
        "Working_time_min", "Target_Per_Hr", "Total_Output", "Output(min)",
        "Total_Defect_Pcs", "Defect_Code_List", "Defect(min)",
        "Data_Accuracy_Status", "Issue_List", "Error_Count", "Warning_Count",
    ]

    drop_dup_cols = ["ยอดผลิต", "Defect_Pcs"]

    def clean_summary(df):
        df = df.drop(
            columns=[
                c for c in drop_summary_cols + drop_dup_cols if c in df.columns
            ]
        )
        ordered    = [c for c in SUMMARY_COL_ORDER if c in df.columns]
        remaining  = [c for c in df.columns if c not in ordered]
        return df[ordered + remaining]

    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        workbook = writer.book
        workbook.strings_to_urls     = False
        workbook.strings_to_formulas = False

        raw_export.to_excel(
            writer, sheet_name="Raw_Processed", index=False
        )
        clean_summary(oee_accuracy).to_excel(
            writer, sheet_name="Summary_By_Shift", index=False
        )
        clean_summary(healthy_df).to_excel(
            writer, sheet_name="Healthy_Data", index=False
        )
        clean_summary(warning_df).to_excel(
            writer, sheet_name="Warning", index=False
        )
        clean_summary(error_df).to_excel(
            writer, sheet_name="Error", index=False
        )

    log.info(f"✅ Done. Output: {output_file}")
    return str(output_file)