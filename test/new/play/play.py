import re
import time
from pathlib import Path
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
APP_URL = "http://127.0.0.1:5000"
EXCEL_PATH = "C:\\Users\\chand\\OneDrive\\Desktop\\Chandra\\Resume\\MED\\SSH\\ui_automation_inputs_all_6_tabs.xlsx"  # <-- update
HEADLESS = False

# Tab names exactly as your UI shows on top navigation
TAB_LABELS = {
    "data_comparison": ["Data Comparison", "Data Comparision"],
    "data_load": ["DB to DB Load", "Data Load"],
    "schema_generation": ["Schema Generation", "Schema Comparison"],
    "file_load": ["File Load"],
    "file_download": ["File Download"],
    "mismatch_explorer": ["Mismatch Explorer"],
}

SHEETS = {
    "data_comparison": "DataComparison_Input",
    "data_load": "DataLoad_Input",
    "schema_generation": "SchemaGeneration_Input",
    "file_load": "FileLoad_Input",
    "file_download": "FileDownload_Input",
    "mismatch_explorer": "MismatchExplorer_Input",
}

# When do we consider "execution completed"?
# Your latest requirement: when Download button / status becomes available.
COMPLETION_SIGNALS = [
    r"Download Comparison Report",
    r"Download Report",
    r"Data load completed",
    r"Execution completed successfully",
    r"✅",
    r"❌",
]

# =========================
# Small helpers
# =========================
def norm(v):
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "null") else s

def _nk(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def safe_bool(v) -> bool:
    return _nk(norm(v)) in ("1", "true", "yes", "y")

def wait_network_idle(page, ms=500):
    page.wait_for_timeout(ms)

def scroll_into_view(locator):
    try:
        locator.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass

def is_visible(locator) -> bool:
    try:
        return locator.is_visible()
    except Exception:
        return False

def tag_name(locator) -> str:
    try:
        return locator.evaluate("e => e.tagName.toLowerCase()")
    except Exception:
        return ""

# =========================
# Robust UI finders
# =========================
def click_tab(page, tab_key: str):
    """
    Click a top tab by text. Tries multiple label variants.
    """
    for label in TAB_LABELS.get(tab_key, []):
        loc = page.get_by_role("button", name=re.compile(label, re.I))
        if loc.count() > 0:
            loc.first.click()
            wait_network_idle(page, 500)
            return
        loc2 = page.get_by_text(label, exact=False)
        if loc2.count() > 0:
            loc2.first.click()
            wait_network_idle(page, 500)
            return
    raise RuntimeError(f"Could not open tab: {tab_key}. Update TAB_LABELS mapping.")

def find_card(page, title_text: str):
    """
    Finds the card container by title text.
    """
    title = page.get_by_text(title_text, exact=False).first
    scroll_into_view(title)
    return title.locator("xpath=ancestor::div[.//input or .//select or .//textarea][1]")

def find_control_in_card(card, label_text: str):
    """
    Within a card, locate the input/select/textarea near label text.
    """
    lab = card.get_by_text(label_text, exact=False).first
    scroll_into_view(lab)
    # Pick nearest container then input/select/textarea
    ctrl = lab.locator(
        "xpath=ancestor::*[self::div or self::section or self::tr][1]"
        "//input | ancestor::*[self::div or self::section or self::tr][1]//select | "
        "ancestor::*[self::div or self::section or self::tr][1]//textarea"
    ).first
    return ctrl

def select_fuzzy(locator, desired: str, timeout_ms: int = 30000):
    desired = norm(desired)
    if not desired:
        return False

    locator.wait_for(state="visible", timeout=timeout_ms)
    scroll_into_view(locator)

    options = locator.locator("option")
    dk = _nk(desired)
    best_val = None
    best_lbl = None

    for i in range(options.count()):
        opt = options.nth(i)
        txt = norm(opt.inner_text())
        val = norm(opt.get_attribute("value"))
        if dk and (dk in _nk(txt) or dk in _nk(val)):
            best_val = val
            best_lbl = txt
            break

    if best_val is None and best_lbl is None:
        # try exact label match
        for i in range(options.count()):
            opt = options.nth(i)
            txt = norm(opt.inner_text())
            if txt.lower() == desired.lower():
                best_lbl = txt
                best_val = norm(opt.get_attribute("value"))
                break

    if best_val or best_lbl:
        try:
            if best_val:
                locator.select_option(value=best_val, timeout=timeout_ms)
            else:
                locator.select_option(label=best_lbl, timeout=timeout_ms)
            return True
        except PWTimeout:
            return False

    return False

def set_value(ctrl, value: str):
    value = norm(value)
    if not value:
        return False

    try:
        ctrl.wait_for(state="attached", timeout=5000)
    except Exception:
        return False

    scroll_into_view(ctrl)
    try:
        # ensure visible
        ctrl.wait_for(state="visible", timeout=5000)
    except Exception:
        return False

    t = tag_name(ctrl)
    try:
        if t == "select":
            return select_fuzzy(ctrl, value)
        elif t == "textarea":
            ctrl.fill("")
            ctrl.type(value)
            return True
        else:
            # input
            ctrl.fill("")
            ctrl.type(value)
            return True
    except Exception:
        return False

def set_by_label_candidates(card, label_candidates, value):
    """
    Try multiple possible label names for same field.
    """
    for lab in label_candidates:
        try:
            ctrl = find_control_in_card(card, lab)
            if ctrl.count() > 0 and set_value(ctrl, value):
                return True
        except Exception:
            continue
    return False

def upload_file_in_card(card, file_path: str):
    p = Path(norm(file_path))
    if not p.exists():
        raise FileNotFoundError(f"Upload file not found: {p}")
    inp = card.locator('input[type="file"]').first
    inp.wait_for(state="visible", timeout=15000)
    scroll_into_view(inp)
    inp.set_input_files(str(p))
    return True

# =========================
# Completion watcher
# =========================
def wait_until_completed(page, timeout_sec=180):
    """
    Wait until we see completion signals in page text OR a download button becomes visible.
    """
    start = time.time()
    patts = [re.compile(x, re.I) for x in COMPLETION_SIGNALS]

    while time.time() - start < timeout_sec:
        # 1) If download button exists and enabled -> completed
        dl = page.get_by_role("button", name=re.compile("Download", re.I))
        if dl.count() > 0 and is_visible(dl.first):
            return "SUCCESS_OR_READY"

        # 2) Scan visible page text for patterns
        try:
            text = page.locator("body").inner_text(timeout=3000)
            for p in patts:
                if p.search(text):
                    # could be still running but good enough for your requirement
                    return "SIGNAL_DETECTED"
        except Exception:
            pass

        page.wait_for_timeout(1500)

    return "TIMEOUT"

# =========================
# Tab Handlers
# =========================
def handle_data_comparison(page, row: dict):
    # Comparison type dropdown always exists in this tab
    comp_type = norm(row.get("comparison_type"))
    if comp_type:
        ctrl = page.locator("#comparison-type-select").first
        ctrl.wait_for(state="visible", timeout=30000)
        select_fuzzy(ctrl, comp_type)

    # Table mode: sometimes not visible (depends on comp type)
    table_mode = norm(row.get("table_mode"))
    if table_mode:
        # Try click label without waiting for visibility (force after scroll)
        for lab in ["Single Table", "Multi Table", "Multiple Tables"]:
            if _nk(table_mode).startswith("single") and "Single" in lab:
                loc = page.get_by_text(lab, exact=False).first
                scroll_into_view(loc)
                try:
                    loc.click(timeout=5000, force=True)
                except Exception:
                    pass
            if _nk(table_mode).startswith(("multi", "multiple")) and "Multi" in lab:
                loc = page.get_by_text(lab, exact=False).first
                scroll_into_view(loc)
                try:
                    loc.click(timeout=5000, force=True)
                except Exception:
                    pass

    # Cards depend on comp type
    # Map based on your UI
    comp_key = _nk(comp_type)
    if "filetodb" in comp_key or "file_to_database" in comp_type.lower():
        src_title = "Source File Configuration"
        tgt_title = "Target Configuration"
    elif "dbtofile" in comp_key or "database_to_file" in comp_type.lower():
        src_title = "Source Configuration"
        tgt_title = "Target File Configuration"
    elif "filetofile" in comp_key or "file_to_file" in comp_type.lower():
        src_title = "Source File Configuration"
        tgt_title = "Target File Configuration"
    else:
        src_title = "Source Configuration"
        tgt_title = "Target Configuration"

    src = find_card(page, src_title)
    tgt = find_card(page, tgt_title)

    # --- Source side ---
    if "Source File" in src_title:
        # File Source fields
        set_by_label_candidates(src, ["File Type"], row.get("file_type"))
        set_by_label_candidates(src, ["Location Type"], row.get("location_type"))

        # If location is Unix or Hadoop -> fill file path field
        # If Upload File -> upload file from excel
        loc = norm(row.get("location_type")).lower()
        if "upload" in loc:
            # upload
            upload_file_in_card(src, row.get("upload_file_path"))
        else:
            # paths
            set_by_label_candidates(src, ["Unix File Path", "File Path (Unix)", "Unix Path", "File Path"], row.get("unix_file_path"))
            set_by_label_candidates(src, ["Hadoop File Path", "File Path (Hadoop)", "Hadoop Path", "HDFS Path"], row.get("hadoop_file_path"))

        set_by_label_candidates(src, ["Delimiter"], row.get("delimiter"))
        set_by_label_candidates(src, ["SQL Query (Optional)", "SQL Query"], row.get("file_sql_query_optional"))
        set_by_label_candidates(src, ["Key Columns (for matching)", "Key Columns"], row.get("source_primary_key"))
    else:
        # DB Source fields
        set_by_label_candidates(src, ["Database Type"], row.get("source_db_type"))
        set_by_label_candidates(src, ["Host"], row.get("source_host"))
        set_by_label_candidates(src, ["Port"], row.get("source_port"))
        set_by_label_candidates(src, ["Database Name"], row.get("source_database"))
        set_by_label_candidates(src, ["Username"], row.get("source_username"))
        set_by_label_candidates(src, ["Password"], row.get("source_password"))
        set_by_label_candidates(src, ["Table Name"], row.get("source_table_name"))
        set_by_label_candidates(src, ["Primary Key"], row.get("source_primary_key"))
        set_by_label_candidates(src, ["SQL Query (Optional)", "SQL Query"], row.get("source_sql_query_optional"))

    # --- Target side ---
    if "Target File" in tgt_title:
        set_by_label_candidates(tgt, ["File Type"], row.get("target_file_type"))
        set_by_label_candidates(tgt, ["Location Type"], row.get("target_location_type"))
        set_by_label_candidates(tgt, ["Delimiter"], row.get("target_delimiter"))
        set_by_label_candidates(tgt, ["File Path", "Target File Path"], row.get("target_file_path"))
    else:
        set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
        set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
        set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
        set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
        set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
        set_by_label_candidates(tgt, ["Password"], row.get("target_password"))
        set_by_label_candidates(tgt, ["Table Name"], row.get("target_table_name"))
        set_by_label_candidates(tgt, ["Primary Key"], row.get("target_primary_key"))
        set_by_label_candidates(tgt, ["SQL Query (Optional)", "SQL Query"], row.get("target_sql_query_optional"))

    # Time zone (should exist for all selections)
    tz_card = find_card(page, "Time Zone")
    set_by_label_candidates(tz_card, ["Time Zone"], row.get("time_zone"))

def handle_data_load(page, row: dict):
    # Source and Target are DB cards
    src = find_card(page, "Source Configuration")
    tgt = find_card(page, "Target Configuration")

    set_by_label_candidates(src, ["Database Type"], row.get("source_db_type"))
    set_by_label_candidates(src, ["Host"], row.get("source_host"))
    set_by_label_candidates(src, ["Port"], row.get("source_port"))
    set_by_label_candidates(src, ["Database Name"], row.get("source_database"))
    set_by_label_candidates(src, ["Username"], row.get("source_username"))
    set_by_label_candidates(src, ["Password"], row.get("source_password"))
    set_by_label_candidates(src, ["Table Name"], row.get("source_table_name"))

    set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
    set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
    set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
    set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
    set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
    set_by_label_candidates(tgt, ["Password"], row.get("target_password"))
    set_by_label_candidates(tgt, ["Table Name"], row.get("target_table_name"))
    set_by_label_candidates(tgt, ["Load Type"], row.get("target_load_type"))  # overwrite/append

    # Hive extra fields (if shown)
    set_by_label_candidates(tgt, ["Hadoop Path"], row.get("target_hadoop_path"))
    set_by_label_candidates(tgt, ["Partition ID"], row.get("target_partition_id"))

    tz_card = find_card(page, "Time Zone")
    set_by_label_candidates(tz_card, ["Time Zone"], row.get("time_zone"))

def handle_schema_generation(page, row: dict):
    src = find_card(page, "Source Configuration")
    tgt = find_card(page, "Target Configuration")

    # Source DB creds
    set_by_label_candidates(src, ["Database Type"], row.get("source_db_type"))
    set_by_label_candidates(src, ["Host"], row.get("source_host"))
    set_by_label_candidates(src, ["Port"], row.get("source_port"))
    set_by_label_candidates(src, ["Database Name"], row.get("source_database"))
    set_by_label_candidates(src, ["Username"], row.get("source_username"))
    set_by_label_candidates(src, ["Password"], row.get("source_password"))

    # Mode (Single/Multiple) after password
    set_by_label_candidates(src, ["Schema Mode", "Schema for single or multiple"], row.get("schema_mode"))

    if _nk(row.get("schema_mode")).startswith("single"):
        set_by_label_candidates(src, ["Table Name"], row.get("source_table_name"))
    else:
        # multiple -> upload list file
        upload_file_in_card(src, row.get("source_upload_file_path"))

    # Target DB creds
    set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
    set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
    set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
    set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
    set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
    set_by_label_candidates(tgt, ["Password"], row.get("target_password"))

    # Target file name mandatory
    set_by_label_candidates(tgt, ["File Name", "Target File Name"], row.get("target_file_name"))

def handle_file_load(page, row: dict):
    # Source is File card, Target is DB card (similar to Data Load target)
    src = find_card(page, "Source File Configuration")
    tgt = find_card(page, "Target Configuration")

    set_by_label_candidates(src, ["File Type"], row.get("file_type"))
    set_by_label_candidates(src, ["Location Type"], row.get("location_type"))

    loc = norm(row.get("location_type")).lower()
    if "upload" in loc:
        upload_file_in_card(src, row.get("upload_file_path"))
    else:
        set_by_label_candidates(src, ["Unix File Path", "File Path (Unix)", "Unix Path"], row.get("unix_file_path"))
        set_by_label_candidates(src, ["Hadoop File Path", "File Path (Hadoop)", "Hadoop Path", "HDFS Path"], row.get("hadoop_file_path"))

    set_by_label_candidates(src, ["Delimiter"], row.get("delimiter"))
    set_by_label_candidates(src, ["SQL Query (Optional)", "SQL Query"], row.get("file_sql_query_optional"))
    set_by_label_candidates(src, ["Key Columns (for matching)", "Key Columns"], row.get("key_columns"))

    # Target (Data Load target style)
    set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
    set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
    set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
    set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
    set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
    set_by_label_candidates(tgt, ["Password"], row.get("target_password"))
    set_by_label_candidates(tgt, ["Table Name"], row.get("target_table_name"))
    set_by_label_candidates(tgt, ["Load Type"], row.get("target_load_type"))

    # Hive extras if visible
    set_by_label_candidates(tgt, ["Hadoop Path"], row.get("target_hadoop_path"))
    set_by_label_candidates(tgt, ["Partition ID"], row.get("target_partition_id"))

    tz_card = find_card(page, "Time Zone")
    set_by_label_candidates(tz_card, ["Time Zone"], row.get("time_zone"))

def handle_file_download(page, row: dict):
    # Source side can be File or DB, Target is File
    src_file = find_card(page, "Source File Configuration")
    src_db = find_card(page, "Source Configuration")
    tgt = find_card(page, "Target File Configuration")

    # A field like "Source Type" or "From" decides which block is visible.
    # Try to set whichever exists.
    # If your UI uses "From" dropdown, put it here.
    # Otherwise skip and only fill the visible section.
    from_type = norm(row.get("from_type")).lower()

    if "file" in from_type:
        set_by_label_candidates(src_file, ["File Type"], row.get("source_file_type"))
        set_by_label_candidates(src_file, ["Location Type"], row.get("source_location_type"))
        loc = norm(row.get("source_location_type")).lower()
        if "upload" in loc:
            upload_file_in_card(src_file, row.get("source_upload_file_path"))
        else:
            set_by_label_candidates(src_file, ["Unix File Path", "File Path (Unix)"], row.get("source_unix_file_path"))
            set_by_label_candidates(src_file, ["Hadoop File Path", "File Path (Hadoop)"], row.get("source_hadoop_file_path"))
    else:
        set_by_label_candidates(src_db, ["Database Type"], row.get("source_db_type"))
        set_by_label_candidates(src_db, ["Host"], row.get("source_host"))
        set_by_label_candidates(src_db, ["Port"], row.get("source_port"))
        set_by_label_candidates(src_db, ["Database Name"], row.get("source_database"))
        set_by_label_candidates(src_db, ["Username"], row.get("source_username"))
        set_by_label_candidates(src_db, ["Password"], row.get("source_password"))
        set_by_label_candidates(src_db, ["Table Name"], row.get("source_table_name"))
        set_by_label_candidates(src_db, ["SQL Query (Optional)", "SQL Query"], row.get("source_sql_query_optional"))

    # Target file always
    set_by_label_candidates(tgt, ["Target File Type", "File Type"], row.get("target_file_type"))
    set_by_label_candidates(tgt, ["Delimiter"], row.get("target_delimiter"))
    set_by_label_candidates(tgt, ["Target File Name", "File Name"], row.get("target_file_name"))
    set_by_label_candidates(tgt, ["Number of Rows"], row.get("num_rows"))

def handle_mismatch_explorer(page, row: dict):
    src = find_card(page, "Source Configuration")
    tgt = find_card(page, "Target Configuration")
    rules = find_card(page, "Comparison Rules")

    # Similar to Data Comparison DB-DB
    set_by_label_candidates(src, ["Database Type"], row.get("source_db_type"))
    set_by_label_candidates(src, ["Host"], row.get("source_host"))
    set_by_label_candidates(src, ["Port"], row.get("source_port"))
    set_by_label_candidates(src, ["Database Name"], row.get("source_database"))
    set_by_label_candidates(src, ["Username"], row.get("source_username"))
    set_by_label_candidates(src, ["Password"], row.get("source_password"))
    set_by_label_candidates(src, ["Table Name"], row.get("source_table_name"))
    set_by_label_candidates(src, ["Primary Key"], row.get("pk_column"))

    set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
    set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
    set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
    set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
    set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
    set_by_label_candidates(tgt, ["Password"], row.get("target_password"))
    set_by_label_candidates(tgt, ["Table Name"], row.get("target_table_name"))
    set_by_label_candidates(tgt, ["Primary Key"], row.get("pk_column"))

    set_by_label_candidates(rules, ["Time Zone"], row.get("time_zone"))
    set_by_label_candidates(rules, ["Trim Spaces"], row.get("trim_spaces"))
    set_by_label_candidates(rules, ["Ignore Case"], row.get("ignore_case"))
    set_by_label_candidates(rules, ["Null equals empty"], row.get("null_equals_empty"))
    set_by_label_candidates(rules, ["Numeric tolerance", "Tolerance"], row.get("tolerance"))

# =========================
# Unix block (common)
# =========================
def fill_unix_block(page, row: dict):
    unix = find_card(page, "Unix Server Configuration")

    # Your new layout: 3 rows x 2 fields (host+cluster, mail+batch, user+pwd)
    set_by_label_candidates(unix, ["Unix Host", "Host"], row.get("unix_host"))
    set_by_label_candidates(unix, ["Cluster Type"], row.get("cluster_type"))
    set_by_label_candidates(unix, ["User Mail", "User Email"], row.get("user_mail"))
    set_by_label_candidates(unix, ["Batch ID"], row.get("batch_id"))
    set_by_label_candidates(unix, ["Unix Username"], row.get("unix_username"))

    # OTP input from terminal, fill into UI password field
    otp = input("Enter Unix Password (OTP) and press ENTER: ").strip()
    if otp:
        set_by_label_candidates(unix, ["Unix Password"], otp)

def click_execute(page):
    # Different tabs use different execute button labels – try common ones
    candidates = [
        r"Execute",
        r"Execute Data Comparison",
        r"Execute Data Load",
        r"Execute Schema",
        r"Execute File Load",
        r"Execute File Download",
    ]
    for c in candidates:
        btn = page.get_by_role("button", name=re.compile(c, re.I))
        if btn.count() > 0:
            scroll_into_view(btn.first)
            btn.first.click()
            return True
    return False

# =========================
# MAIN
# =========================
def run_tab(page, tab_key: str, df: pd.DataFrame):
    click_tab(page, tab_key)

    for idx in range(len(df)):
        row = df.iloc[idx].to_dict()

        # Fill tab-specific
        if tab_key == "data_comparison":
            handle_data_comparison(page, row)
        elif tab_key == "data_load":
            handle_data_load(page, row)
        elif tab_key == "schema_generation":
            handle_schema_generation(page, row)
        elif tab_key == "file_load":
            handle_file_load(page, row)
        elif tab_key == "file_download":
            handle_file_download(page, row)
        elif tab_key == "mismatch_explorer":
            handle_mismatch_explorer(page, row)
        else:
            raise RuntimeError(f"Unknown tab key: {tab_key}")

        # Fill unix block (common in all tabs)
        fill_unix_block(page, row)

        # Execute
        if not click_execute(page):
            raise RuntimeError("Execute button not found. Check button text in UI.")

        print("▶ Execution started... waiting for completion signal (download/status/log)...")
        status = wait_until_completed(page, timeout_sec=240)
        print(f"✔ Completion watcher result: {status}")

        # Stop after first row by default (you can remove this if you want full iteration)
        break

def main():
    # Choose which tab to run
    tab_key = input(
        "Enter tab key to run (data_comparison/data_load/schema_generation/file_load/file_download/mismatch_explorer): "
    ).strip().lower()

    if tab_key not in SHEETS:
        raise ValueError(f"Invalid tab key: {tab_key}. Options: {list(SHEETS.keys())}")

    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEETS[tab_key])
    if df.empty:
        raise ValueError(f"No rows found in sheet: {SHEETS[tab_key]}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()
        page.goto(APP_URL, wait_until="networkidle")

        run_tab(page, tab_key, df)

        input("Execution completed (or timed out). Press ENTER to close browser...")
        browser.close()

if __name__ == "__main__":
    main()
