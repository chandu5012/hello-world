import re
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
APP_URL = "http://127.0.0.1:5000"
EXCEL_PATH = "C:\\Users\\chand\\OneDrive\\Desktop\\Chandra\\Resume\\MED\\SSH\\ui_automation_inputs_all_6_tabs.xlsx"
HEADLESS = False
DEFAULT_TIMEOUT_MS = 45000

TAB_LABELS = {
    "data_comparison": ["Data Comparison", "Data Comparision"],
    "data_load": ["DB to DB Load", "DB to DB load", "Data Load"],
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

# Used only for "job completion" check (lightweight)
COMPLETION_SIGNALS = [
    r"Download Comparison Report",
    r"Download Report",
    r"Download",
    r"completed successfully",
    r"Execution completed",
    r"✅",
    r"❌",
]

# Tab “anchors” to confirm tab is really opened (must be VISIBLE)
TAB_ANCHORS = {
    "data_comparison": ["Execute Data Comparison", "Real-time Logs - Data Comparison", "Source Configuration"],
    "data_load": ["Execute Data Load", "Real-time Logs - Data Load", "Source Configuration"],
    "schema_generation": ["Schema Generation", "Execute Schema", "Target Configuration"],
    "file_load": ["File Load", "Execute File Load"],  # (kept as-is; not changing other logic)
    "file_download": ["File Download", "Execute File Download", "Target File Configuration"],
    "mismatch_explorer": ["Mismatch Explorer", "Comparison Rules", "Real-time Logs - Mismatch Explorer"],
}


# =========================
# Helpers
# =========================
def norm(v):
    if v is None:
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "null") else s


def _nk(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def is_visible(locator) -> bool:
    try:
        return locator.is_visible()
    except Exception:
        return False


def scroll_into_view(locator):
    try:
        locator.scroll_into_view_if_needed(timeout=5000)
    except Exception:
        pass


def tag_name(locator) -> str:
    try:
        return locator.evaluate("e => e.tagName.toLowerCase()")
    except Exception:
        return ""


def launch_browser(p, browser_name: str, headless: bool):
    b = (browser_name or "").strip().lower()
    args = ["--start-maximized"]
    if b in ("msedge", "edge"):
        return p.chromium.launch(channel="msedge", headless=headless, args=args)
    if b in ("chrome", "google-chrome"):
        return p.chromium.launch(channel="chrome", headless=headless, args=args)
    return p.chromium.launch(headless=headless, args=args)


def pick_first_visible(locator):
    """Given a locator with multiple matches, return the first visible element (as locator.nth(i))."""
    try:
        cnt = locator.count()
    except Exception:
        return locator.first
    for i in range(cnt):
        li = locator.nth(i)
        if is_visible(li):
            return li
    return locator.first


def safe_click(locator, timeout_ms=8000) -> bool:
    try:
        locator.wait_for(state="attached", timeout=timeout_ms)
    except Exception:
        return False
    scroll_into_view(locator)
    try:
        locator.click(timeout=timeout_ms, force=True)
        return True
    except Exception:
        return False


# =========================
# ✅ Enter Manually Support (ADDED ONLY)
# =========================
# These are the common option texts used in your dropdowns to reveal a manual input.
_ENTER_MANUAL_TOKENS = {
    "entermanually",
    "enter_manual",
    "manual",
    "enter",
    "entermanual",
    "enter_manually",
}
_ENTER_MANUAL_TEXT_CANDIDATES = [
    "Enter Manually",
    "Enter manually",
    "Enter Manual",
    "Manual",
    "Enter",
]

def _is_manual_option_text(txt: str) -> bool:
    k = _nk(txt)
    if not k:
        return False
    if k in _ENTER_MANUAL_TOKENS:
        return True
    # also allow "enter manually" as substring
    return "entermanually" in k or "manual" == k

def _select_enter_manually(select_locator) -> bool:
    """
    Select the 'Enter Manually/Manual' option from a <select>.
    Returns True if selection happened.
    """
    try:
        select_locator.wait_for(state="visible", timeout=DEFAULT_TIMEOUT_MS)
    except Exception:
        return False

    opts = select_locator.locator("option")
    try:
        cnt = opts.count()
    except Exception:
        cnt = 0

    best_val = None
    best_lbl = None
    for i in range(cnt):
        opt = opts.nth(i)
        try:
            lbl = norm(opt.inner_text())
            val = norm(opt.get_attribute("value"))
        except Exception:
            continue
        if _is_manual_option_text(lbl) or _is_manual_option_text(val):
            best_lbl = lbl
            best_val = val
            break

    if not (best_lbl or best_val):
        # fallback: try select by label candidates directly
        for cand in _ENTER_MANUAL_TEXT_CANDIDATES:
            try:
                select_locator.select_option(label=cand, timeout=3000)
                return True
            except Exception:
                continue
        return False

    try:
        if best_val:
            select_locator.select_option(value=best_val, timeout=DEFAULT_TIMEOUT_MS)
        else:
            select_locator.select_option(label=best_lbl, timeout=DEFAULT_TIMEOUT_MS)
        return True
    except Exception:
        return False

def _find_manual_input_near_select(select_locator):
    """
    After choosing 'Enter Manually', UI usually reveals an input near the select.
    We find a visible, enabled input/textarea near that select (same container / next container).
    """
    # 1) same immediate wrapper
    wrappers = [
        select_locator.locator("xpath=ancestor::div[1]"),
        select_locator.locator("xpath=ancestor::div[2]"),
        select_locator.locator("xpath=ancestor::div[3]"),
    ]

    # input filters: ignore file/checkbox/radio/hidden
    def _valid_input(inp):
        try:
            if not is_visible(inp):
                return False
            t = (inp.get_attribute("type") or "").lower()
            if t in ("file", "checkbox", "radio", "hidden", "submit", "button"):
                return False
            if inp.is_disabled():
                return False
            return True
        except Exception:
            return False

    for w in wrappers:
        try:
            # Prefer input/textarea inside same wrapper but not the select itself
            cand = w.locator("input, textarea")
            for i in range(cand.count()):
                ci = cand.nth(i)
                if _valid_input(ci):
                    return ci
        except Exception:
            pass

    # 2) following input/textarea in DOM
    try:
        following = select_locator.locator("xpath=following::input[1] | following::textarea[1]")
        if following.count() > 0:
            fv = pick_first_visible(following)
            if _valid_input(fv):
                return fv
    except Exception:
        pass

    return None


# =========================
# Robust: tabs
# =========================
def wait_for_any_visible_text(page, texts, timeout_ms=DEFAULT_TIMEOUT_MS):
    end = time.time() + (timeout_ms / 1000.0)
    while time.time() < end:
        for t in texts:
            loc = page.get_by_text(t, exact=False)
            if loc.count() > 0:
                locv = pick_first_visible(loc)
                if is_visible(locv):
                    return True
        page.wait_for_timeout(250)
    return False


def click_tab(page, tab_key: str):
    labels = TAB_LABELS.get(tab_key, [])
    if not labels:
        raise RuntimeError(f"Unknown tab_key '{tab_key}'. Update TAB_LABELS.")

    # Try role=tab, then role=button, then plain text
    for label in labels:
        for role in ("tab", "button"):
            loc = page.get_by_role(role, name=re.compile(label, re.I))
            if loc.count() > 0:
                lv = pick_first_visible(loc)
                if safe_click(lv, timeout_ms=DEFAULT_TIMEOUT_MS):
                    page.wait_for_timeout(400)
                    if wait_for_any_visible_text(page, TAB_ANCHORS.get(tab_key, [label]), timeout_ms=15000):
                        return

        loc2 = page.get_by_text(label, exact=False)
        if loc2.count() > 0:
            lv2 = pick_first_visible(loc2)
            if safe_click(lv2, timeout_ms=DEFAULT_TIMEOUT_MS):
                page.wait_for_timeout(400)
                if wait_for_any_visible_text(page, TAB_ANCHORS.get(tab_key, [label]), timeout_ms=15000):
                    return

    raise RuntimeError(f"Could not open tab: {tab_key}. Update TAB_LABELS/TAB_ANCHORS.")


# =========================
# Robust: cards and controls (VISIBLE ONLY)
# =========================
def find_card(page, title_text: str):
    """
    Find the VISIBLE card container by title text.
    """
    titles = page.get_by_text(title_text, exact=False)
    if titles.count() == 0:
        raise RuntimeError(f"Card title not found: {title_text}")

    title = pick_first_visible(titles)
    scroll_into_view(title)

    # Find nearest ancestor with form controls
    card = title.locator("xpath=ancestor::div[.//input or .//select or .//textarea][1]")
    if card.count() == 0:
        raise RuntimeError(f"Could not resolve card container for title: {title_text}")
    return card.first


def find_card_any(page, possible_titles):
    """
    Try multiple card titles and return the first VISIBLE one.
    """
    last_err = None
    for t in possible_titles:
        try:
            return find_card(page, t)
        except Exception as e:
            last_err = e
            continue
    raise RuntimeError(f"None of the card titles found/visible: {possible_titles}. Last error: {last_err}")


def find_control_in_card(card, label_text: str):
    """
    Find VISIBLE control near label text inside a card.
    Works across Edge/Chrome by skipping hidden matches.
    """
    labs = card.get_by_text(label_text, exact=False)
    if labs.count() == 0:
        return None

    lab = pick_first_visible(labs)
    scroll_into_view(lab)

    # Strategy:
    # 1) nearest div wrapper -> input/select/textarea
    w1 = lab.locator("xpath=ancestor::div[1]")
    c1 = w1.locator("input, select, textarea")
    if c1.count() > 0:
        c1v = pick_first_visible(c1)
        if is_visible(c1v):
            return c1v

    # 2) ancestor div[2]
    w2 = lab.locator("xpath=ancestor::div[2]")
    c2 = w2.locator("input, select, textarea")
    if c2.count() > 0:
        c2v = pick_first_visible(c2)
        if is_visible(c2v):
            return c2v

    # 3) following controls
    c3 = lab.locator("xpath=following::input[1] | following::select[1] | following::textarea[1]")
    if c3.count() > 0:
        c3v = pick_first_visible(c3)
        if is_visible(c3v):
            return c3v

    return None


def select_fuzzy(select_locator, desired: str, timeout_ms: int = DEFAULT_TIMEOUT_MS):
    desired = norm(desired)
    if not desired:
        return False

    try:
        select_locator.wait_for(state="visible", timeout=timeout_ms)
    except Exception:
        return False

    scroll_into_view(select_locator)
    dk = _nk(desired)

    options = select_locator.locator("option")
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

    if not (best_val or best_lbl):
        # exact fallback
        for i in range(options.count()):
            opt = options.nth(i)
            txt = norm(opt.inner_text())
            if txt.lower() == desired.lower():
                best_lbl = txt
                best_val = norm(opt.get_attribute("value"))
                break

    if not (best_val or best_lbl):
        print(f"[WARN] Could not match select '{desired}'")
        return False

    try:
        if best_val:
            select_locator.select_option(value=best_val, timeout=timeout_ms)
        else:
            select_locator.select_option(label=best_lbl, timeout=timeout_ms)
        return True
    except PWTimeout:
        return False
    except Exception:
        return False


def set_value(ctrl, value: str, field_name: str = ""):
    """
    UPDATED (only): If ctrl is <select> and desired value is not in options,
    auto-select "Enter Manually" and fill the revealed input near the select.
    """
    value = norm(value)
    if not value or ctrl is None:
        return False

    try:
        ctrl.wait_for(state="attached", timeout=15000)
    except Exception:
        print(f"[WARN] ctrl not attached for field='{field_name}'")
        return False

    scroll_into_view(ctrl)

    try:
        ctrl.wait_for(state="visible", timeout=15000)
    except Exception:
        print(f"[WARN] ctrl not visible for field='{field_name}'")
        return False

    # Edge: focus first helps a lot
    try:
        ctrl.click(timeout=2500, force=True)
    except Exception:
        pass

    t = tag_name(ctrl)
    try:
        if t == "select":
            # 1) normal select
            ok = select_fuzzy(ctrl, value)
            if ok:
                return True

            # 2) Enter Manually fallback (NEW)
            # Try selecting "Enter Manually", then fill nearby revealed input with actual value
            if _select_enter_manually(ctrl):
                # allow UI to render manual input
                try:
                    ctrl.page.wait_for_timeout(250)
                except Exception:
                    pass

                manual_inp = _find_manual_input_near_select(ctrl)
                if manual_inp is not None:
                    try:
                        manual_inp.click(timeout=2000, force=True)
                    except Exception:
                        pass
                    try:
                        manual_inp.fill("")
                        manual_inp.type(value, delay=8)
                        return True
                    except Exception as e:
                        print(f"[WARN] manual fill failed field='{field_name}' value='{value}' err={e}")
                        return False

            print(f"[WARN] select failed field='{field_name}' value='{value}' (no match, manual not available)")
            return False

        # normal input/textarea
        ctrl.fill("")
        ctrl.type(value, delay=8)
        return True

    except Exception as e:
        print(f"[WARN] set_value failed field='{field_name}' value='{value}' err={e}")
        return False


def set_by_label_candidates(card, label_candidates, value):
    value = norm(value)
    if not value:
        return False

    for lab in label_candidates:
        try:
            ctrl = find_control_in_card(card, lab)
            if ctrl and set_value(ctrl, value, field_name=lab):
                return True
        except Exception:
            continue

    print(f"[WARN] control not found/filled labels={label_candidates} value='{value}'")
    return False


def upload_file_in_card(card, file_path: str):
    p = Path(norm(file_path))
    if not p.exists():
        raise FileNotFoundError(f"Upload file not found: {p}")

    inp = card.locator('input[type="file"]')
    if inp.count() == 0:
        raise RuntimeError("File input not found in this card.")
    inv = pick_first_visible(inp)
    inv.wait_for(state="visible", timeout=DEFAULT_TIMEOUT_MS)
    scroll_into_view(inv)
    inv.set_input_files(str(p))
    return True


# =========================
# Completion watcher (fast)
# =========================
def wait_until_completed(page, timeout_sec=240):
    start = time.time()
    patts = [re.compile(x, re.I) for x in COMPLETION_SIGNALS]

    while time.time() - start < timeout_sec:
        dl = page.get_by_role("button", name=re.compile("Download", re.I))
        if dl.count() > 0:
            dlv = pick_first_visible(dl)
            if is_visible(dlv):
                return "DOWNLOAD_READY"

        # lightweight log area scan (instead of whole body)
        log_box = page.locator("pre, code, textarea").first
        try:
            txt = log_box.inner_text(timeout=1000) if log_box.count() > 0 else ""
            for p in patts:
                if p.search(txt):
                    return "SIGNAL_DETECTED"
        except Exception:
            pass

        page.wait_for_timeout(1200)

    return "TIMEOUT"


# =========================
# Per-tab handlers
# =========================
def set_table_mode_if_visible(page, table_mode: str):
    tm = _nk(norm(table_mode))
    if not tm:
        return

    want_multi = tm.startswith("multi") or tm.startswith("multiple")
    want_single = tm.startswith("single")

    candidates = ["Single Table", "Multi Table", "Multiple Tables", "Multiple Table"]
    for c in candidates:
        loc = page.get_by_text(c, exact=False)
        if loc.count() == 0:
            continue
        lv = pick_first_visible(loc)
        if not is_visible(lv):
            continue

        if want_single and "Single" in c:
            safe_click(lv, timeout_ms=8000)
            page.wait_for_timeout(300)
            return
        if want_multi and ("Multi" in c or "Multiple" in c):
            safe_click(lv, timeout_ms=8000)
            page.wait_for_timeout(300)
            return

    print("[INFO] Table Mode not visible. Skipping.")


# --- KEEP your working tabs unchanged ---
def handle_data_comparison(page, row: dict):
    comp_type = norm(row.get("comparison_type"))
    if comp_type:
        ctrl = page.locator("#comparison-type-select")
        if ctrl.count() > 0:
            cv = pick_first_visible(ctrl)
            select_fuzzy(cv, comp_type)
            page.wait_for_timeout(300)

    set_table_mode_if_visible(page, row.get("table_mode"))
    page.wait_for_timeout(300)

    comp_key = _nk(comp_type)
    if "filetodb" in comp_key or "file_to_database" in comp_type.lower():
        src_title, tgt_title = "Source File Configuration", "Target Configuration"
    elif "dbtofile" in comp_key or "database_to_file" in comp_type.lower():
        src_title, tgt_title = "Source Configuration", "Target File Configuration"
    elif "filetofile" in comp_key or "file_to_file" in comp_type.lower():
        src_title, tgt_title = "Source File Configuration", "Target File Configuration"
    else:
        src_title, tgt_title = "Source Configuration", "Target Configuration"

    src = find_card(page, src_title)
    tgt = find_card(page, tgt_title)

    is_multi = _nk(row.get("table_mode")).startswith(("multi", "multiple"))

    if "Source File" in src_title:
        set_by_label_candidates(src, ["File Type"], row.get("file_type"))
        set_by_label_candidates(src, ["Location Type"], row.get("location_type"))
        loc = norm(row.get("location_type")).lower()
        if "upload" in loc:
            upload_file_in_card(src, row.get("upload_file_path"))
        else:
            if "unix" in loc:
                set_by_label_candidates(src, ["File Path (Unix)", "Unix File Path", "File Path"], row.get("unix_file_path"))
            if "hadoop" in loc or "hdfs" in loc:
                set_by_label_candidates(src, ["File Path (Hadoop)", "Hadoop File Path", "File Path"], row.get("hadoop_file_path"))

        set_by_label_candidates(src, ["Delimiter"], row.get("delimiter"))
        set_by_label_candidates(src, ["SQL Query (Optional)", "SQL Query"], row.get("file_sql_query_optional"))
        set_by_label_candidates(src, ["Key Columns (for matching)", "Key Columns", "Primary Key"], row.get("source_primary_key"))
    else:
        set_by_label_candidates(src, ["Database Type"], row.get("source_db_type"))
        set_by_label_candidates(src, ["Host"], row.get("source_host"))
        set_by_label_candidates(src, ["Port"], row.get("source_port"))
        set_by_label_candidates(src, ["Database Name"], row.get("source_database"))
        set_by_label_candidates(src, ["Username"], row.get("source_username"))
        set_by_label_candidates(src, ["Password"], row.get("source_password"))
        if not is_multi:
            set_by_label_candidates(src, ["Table Name"], row.get("source_table_name"))
            set_by_label_candidates(src, ["Primary Key"], row.get("source_primary_key"))
            set_by_label_candidates(src, ["SQL Query (Optional)", "SQL Query"], row.get("source_sql_query_optional"))

    if "Target File" in tgt_title:
        set_by_label_candidates(tgt, ["File Type", "Target File Type"], row.get("target_file_type"))
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
        if not is_multi:
            set_by_label_candidates(tgt, ["Table Name"], row.get("target_table_name"))
            set_by_label_candidates(tgt, ["Primary Key"], row.get("target_primary_key"))
            set_by_label_candidates(tgt, ["SQL Query (Optional)", "SQL Query"], row.get("target_sql_query_optional"))

    if is_multi:
        if norm(row.get("source_excel_path")):
            upload_file_in_card(src, row.get("source_excel_path"))
        if norm(row.get("target_excel_path")):
            upload_file_in_card(tgt, row.get("target_excel_path"))

    for title in ["Timezone Configuration", "Time Zone Configuration", "Time Zone"]:
        try:
            tz = find_card(page, title)
            set_by_label_candidates(tz, ["Time Zone"], row.get("time_zone"))
            set_by_label_candidates(tz, ["Time Zone (Manual)", "Enter Time Zone"], row.get("time_zone_manual"))
            break
        except Exception:
            pass


def handle_data_load(page, row: dict):
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
    set_by_label_candidates(tgt, ["Load Type"], row.get("target_load_type"))

    set_by_label_candidates(tgt, ["Hadoop Path"], row.get("target_hadoop_path"))
    set_by_label_candidates(tgt, ["Partition ID"], row.get("target_partition_id"))

    for title in ["Timezone Configuration", "Time Zone Configuration", "Time Zone"]:
        try:
            tz = find_card(page, title)
            set_by_label_candidates(tz, ["Time Zone"], row.get("time_zone"))
            set_by_label_candidates(tz, ["Time Zone (Manual)", "Enter Time Zone"], row.get("time_zone_manual"))
            break
        except Exception:
            pass


def handle_schema_generation(page, row: dict):
    src = find_card(page, "Source Configuration")
    tgt = find_card(page, "Target Configuration")

    set_by_label_candidates(src, ["Database Type"], row.get("source_db_type"))
    set_by_label_candidates(src, ["Host"], row.get("source_host"))
    set_by_label_candidates(src, ["Port"], row.get("source_port"))
    set_by_label_candidates(src, ["Database Name"], row.get("source_database"))
    set_by_label_candidates(src, ["Username"], row.get("source_username"))
    set_by_label_candidates(src, ["Password"], row.get("source_password"))

    set_by_label_candidates(src, ["Schema Mode", "Schema for single or multiple"], row.get("schema_mode"))

    if _nk(row.get("schema_mode")).startswith("single"):
        set_by_label_candidates(src, ["Table Name"], row.get("source_table_name"))
    else:
        if norm(row.get("source_upload_file_path")):
            upload_file_in_card(src, row.get("source_upload_file_path"))

    set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
    set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
    set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
    set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
    set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
    set_by_label_candidates(tgt, ["Password"], row.get("target_password"))

    set_by_label_candidates(tgt, ["File Name", "Target File Name"], row.get("target_file_name"))


# =========================
# ✅ ONLY FIXED TAB: FILE LOAD (kept as-is from your working code)
# =========================
def _find_file_load_source_block(page):
    """
    File Load in Edge sometimes doesn't have 'Source File Configuration' as a visible title.
    So:
      1) Try common title variants
      2) Fallback: locate by 'File Type' label and take its nearest container with inputs/selects
    """
    # 1) Try title variants first
    try:
        return find_card_any(page, [
            "Source File Configuration",
            "Source File Config",
            "Source File Details",
            "Source File",
            "File Configuration",
            "File Details",
            "Source File Setup",
        ])
    except Exception:
        pass

    # 2) Fallback by label anchor
    lab = page.get_by_text("File Type", exact=False)
    if lab.count() == 0:
        # last fallback: maybe label is "Select File Type"
        lab = page.get_by_text("Select file type", exact=False)

    if lab.count() == 0:
        raise RuntimeError("File Load: Could not locate Source File section (no title match, no 'File Type' label).")

    lvis = pick_first_visible(lab)
    scroll_into_view(lvis)

    # nearest container with controls
    block = lvis.locator("xpath=ancestor::div[.//input or .//select or .//textarea][1]")
    if block.count() == 0:
        block = lvis.locator("xpath=ancestor::div[.//input or .//select or .//textarea][2]")
    if block.count() == 0:
        raise RuntimeError("File Load: Could not resolve Source File section container from label.")
    return block.first


def handle_file_load(page, row: dict):
    # ✅ fixed source block detection
    src = _find_file_load_source_block(page)

    # Target is DB config; keep original title but add safe fallback variants (File Load only)
    try:
        tgt = find_card(page, "Target Configuration")
    except Exception:
        tgt = find_card_any(page, ["Target Configuration", "Target DB Configuration", "Target Details", "Target"])

    # ---- Source file inputs ----
    set_by_label_candidates(src, ["File Type"], row.get("file_type"))
    set_by_label_candidates(src, ["Location Type", "File Location Type", "Source Location Type"], row.get("location_type"))

    loc = norm(row.get("location_type")).lower()
    if "upload" in loc:
        upload_file_in_card(src, row.get("upload_file_path"))
    else:
        if "unix" in loc:
            set_by_label_candidates(src, ["File Path (Unix)", "Unix File Path", "Unix Path", "File Path"], row.get("unix_file_path"))
        if "hadoop" in loc or "hdfs" in loc:
            set_by_label_candidates(src, ["File Path (Hadoop)", "Hadoop File Path", "Hadoop Path", "HDFS Path", "File Path"], row.get("hadoop_file_path"))

    set_by_label_candidates(src, ["Delimiter"], row.get("delimiter"))
    set_by_label_candidates(src, ["SQL Query (Optional)", "SQL Query"], row.get("file_sql_query_optional"))
    set_by_label_candidates(src, ["Key Columns (for matching)", "Key Columns", "Primary Key"], row.get("key_columns"))

    # ---- Target DB inputs ----
    set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
    set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
    set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
    set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
    set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
    set_by_label_candidates(tgt, ["Password"], row.get("target_password"))
    set_by_label_candidates(tgt, ["Table Name"], row.get("target_table_name"))
    set_by_label_candidates(tgt, ["Load Type"], row.get("target_load_type"))

    set_by_label_candidates(tgt, ["Hadoop Path"], row.get("target_hadoop_path"))
    set_by_label_candidates(tgt, ["Partition ID"], row.get("target_partition_id"))

    for title in ["Timezone Configuration", "Time Zone Configuration", "Time Zone"]:
        try:
            tz = find_card(page, title)
            set_by_label_candidates(tz, ["Time Zone"], row.get("time_zone"))
            set_by_label_candidates(tz, ["Time Zone (Manual)", "Enter Time Zone"], row.get("time_zone_manual"))
            break
        except Exception:
            pass


# -------------------------
# File Download (kept as your working code)
# -------------------------
def _find_section_scope(page, heading_candidates):
    for h in heading_candidates:
        try:
            heads = page.get_by_text(h, exact=False)
            if heads.count() == 0:
                continue
            hv = pick_first_visible(heads)
            if not is_visible(hv):
                continue
            scroll_into_view(hv)
            scope = hv.locator("xpath=ancestor::div[.//input or .//select or .//textarea][1]")
            if scope.count() > 0:
                return scope.first
        except Exception:
            continue
    return None


def _set_by_label_candidates_fallback(page, preferred_scope, label_candidates, value):
    if preferred_scope is not None:
        ok = set_by_label_candidates(preferred_scope, label_candidates, value)
        if ok:
            return True
    body_scope = page.locator("body")
    return set_by_label_candidates(body_scope, label_candidates, value)


def _set_nth_visible_label_instance(page, label_text, nth_index, value):
    value = norm(value)
    if not value:
        return False

    labs = page.get_by_text(label_text, exact=False)
    if labs.count() == 0:
        return False

    visibles = []
    for i in range(labs.count()):
        li = labs.nth(i)
        if is_visible(li):
            visibles.append(li)

    if len(visibles) <= nth_index:
        return False

    lab = visibles[nth_index]
    scroll_into_view(lab)

    scope = lab.locator("xpath=ancestor::div[.//input or .//select or .//textarea][1]")
    if scope.count() == 0:
        return False

    ctrl = find_control_in_card(scope.first, label_text)
    if ctrl and set_value(ctrl, value, field_name=f"{label_text}[{nth_index}]"):
        return True
    return False


def handle_file_download(page, row: dict):
    source_type_scope = _find_section_scope(page, ["Source Type", "Select source type"])
    src_scope = _find_section_scope(page, ["Source File Configuration", "Source File", "Source Configuration", "Source"])
    tgt_scope = _find_section_scope(page, ["Target File Configuration", "Target File", "Target Configuration", "Target"])

    _set_by_label_candidates_fallback(
        page,
        source_type_scope,
        ["Source Type", "From Type", "Source/From Type", "Select source type"],
        row.get("from_type")
    )
    page.wait_for_timeout(600)

    from_type = norm(row.get("from_type")).lower()

    src_scope = _find_section_scope(page, ["Source File Configuration", "Source File", "Source Configuration", "Source"]) or src_scope
    tgt_scope = _find_section_scope(page, ["Target File Configuration", "Target File", "Target Configuration", "Target"]) or tgt_scope

    if "file" in from_type:
        _set_by_label_candidates_fallback(page, src_scope, ["File Type"], row.get("source_file_type"))
        _set_by_label_candidates_fallback(page, src_scope, ["Location Type"], row.get("source_location_type"))
        _set_by_label_candidates_fallback(page, src_scope, ["Delimiter"], row.get("source_delimiter"))
        _set_by_label_candidates_fallback(page, src_scope, ["SQL Query (Optional)", "SQL Query"], row.get("source_sql_query_optional"))

        loc = norm(row.get("source_location_type")).lower()
        if "upload" in loc:
            up = norm(row.get("source_upload_file_path"))
            if up:
                try:
                    if src_scope is not None:
                        upload_file_in_card(src_scope, up)
                    else:
                        upload_file_in_card(page.locator("body"), up)
                except Exception:
                    p = Path(norm(up))
                    if p.exists():
                        finp = page.locator('input[type="file"]')
                        if finp.count() > 0:
                            pick_first_visible(finp).set_input_files(str(p))
        else:
            if "unix" in loc:
                _set_by_label_candidates_fallback(
                    page, src_scope,
                    ["File Path (Unix)", "Unix File Path", "Source Unix File Path", "File Path"],
                    row.get("source_unix_file_path")
                )
            if "hadoop" in loc or "hdfs" in loc:
                _set_by_label_candidates_fallback(
                    page, src_scope,
                    ["File Path (Hadoop)", "Hadoop File Path", "Source Hadoop File Path", "File Path"],
                    row.get("source_hadoop_file_path")
                )
    else:
        _set_by_label_candidates_fallback(page, src_scope, ["Database Type"], row.get("source_db_type"))
        _set_by_label_candidates_fallback(page, src_scope, ["Host"], row.get("source_host"))
        _set_by_label_candidates_fallback(page, src_scope, ["Port"], row.get("source_port"))
        _set_by_label_candidates_fallback(page, src_scope, ["Database Name"], row.get("source_database"))
        _set_by_label_candidates_fallback(page, src_scope, ["Username"], row.get("source_username"))
        _set_by_label_candidates_fallback(page, src_scope, ["Password"], row.get("source_password"))
        _set_by_label_candidates_fallback(page, src_scope, ["Table Name"], row.get("source_table_name"))
        _set_by_label_candidates_fallback(page, src_scope, ["SQL Query (Optional)", "SQL Query"], row.get("source_sql_query_optional"))

    page.wait_for_timeout(300)

    ok_tgt_file_type = _set_by_label_candidates_fallback(page, tgt_scope, ["Target File Type", "File Type"], row.get("target_file_type"))
    if not ok_tgt_file_type:
        _set_nth_visible_label_instance(page, "File Type", 1, row.get("target_file_type"))

    ok_tgt_loc_type = _set_by_label_candidates_fallback(page, tgt_scope, ["Target Location Type", "Location Type"], row.get("target_location_type"))
    if not ok_tgt_loc_type:
        _set_nth_visible_label_instance(page, "Location Type", 1, row.get("target_location_type"))

    _set_by_label_candidates_fallback(page, tgt_scope, ["Delimiter", "Target Delimiter"], row.get("target_delimiter"))
    _set_by_label_candidates_fallback(
        page, tgt_scope,
        ["Target File Path", "File Path", "Output Path", "Target Path"],
        row.get("target_file_path")
    )
    _set_by_label_candidates_fallback(page, tgt_scope, ["Target File Name", "File Name"], row.get("target_file_name"))
    _set_by_label_candidates_fallback(page, tgt_scope, ["Number of Rows", "Rows", "Row Count"], row.get("num_rows"))

    tz_scope = _find_section_scope(page, ["Timezone Configuration", "Time Zone Configuration", "Time Zone"])
    if tz_scope is not None:
        _set_by_label_candidates_fallback(page, tz_scope, ["Time Zone"], row.get("time_zone"))
        _set_by_label_candidates_fallback(page, tz_scope, ["Time Zone (Manual)", "Enter Time Zone"], row.get("time_zone_manual"))


def handle_mismatch_explorer(page, row: dict):
    src = find_card(page, "Source Configuration")
    tgt = find_card(page, "Target Configuration")
    rules = find_card(page, "Comparison Rules")

    set_by_label_candidates(src, ["Database Type"], row.get("source_db_type"))
    set_by_label_candidates(src, ["Host"], row.get("source_host"))
    set_by_label_candidates(src, ["Port"], row.get("source_port"))
    set_by_label_candidates(src, ["Database Name"], row.get("source_database"))
    set_by_label_candidates(src, ["Username"], row.get("source_username"))
    set_by_label_candidates(src, ["Password"], row.get("source_password"))
    set_by_label_candidates(src, ["Table Name"], row.get("source_table_name"))
    set_by_label_candidates(src, ["Primary Key", "Key Columns"], row.get("pk_column"))

    set_by_label_candidates(tgt, ["Database Type"], row.get("target_db_type"))
    set_by_label_candidates(tgt, ["Host"], row.get("target_host"))
    set_by_label_candidates(tgt, ["Port"], row.get("target_port"))
    set_by_label_candidates(tgt, ["Database Name"], row.get("target_database"))
    set_by_label_candidates(tgt, ["Username"], row.get("target_username"))
    set_by_label_candidates(tgt, ["Password"], row.get("target_password"))
    set_by_label_candidates(tgt, ["Table Name"], row.get("target_table_name"))
    set_by_label_candidates(tgt, ["Primary Key", "Key Columns"], row.get("pk_column"))

    set_by_label_candidates(rules, ["Time Zone"], row.get("time_zone"))
    set_by_label_candidates(rules, ["Time Zone (Manual)", "Enter Time Zone"], row.get("time_zone_manual"))
    set_by_label_candidates(rules, ["Trim Spaces"], row.get("trim_spaces"))
    set_by_label_candidates(rules, ["Ignore Case"], row.get("ignore_case"))
    set_by_label_candidates(rules, ["Null equals empty", "Null Equals Empty"], row.get("null_equals_empty"))
    set_by_label_candidates(rules, ["Numeric tolerance", "Tolerance"], row.get("tolerance"))


# =========================
# Unix block (common)
# =========================
def fill_unix_block(page, row: dict):
    unix = find_card(page, "Unix Server Configuration")

    set_by_label_candidates(unix, ["Unix Host", "Host"], row.get("unix_host"))
    set_by_label_candidates(unix, ["Cluster Type"], row.get("cluster_type"))
    set_by_label_candidates(unix, ["Cluster Type (Manual)", "Enter Cluster Type"], row.get("cluster_type_manual"))

    set_by_label_candidates(unix, ["User Mail", "User Email"], row.get("user_mail"))
    set_by_label_candidates(unix, ["Batch ID"], row.get("batch_id"))

    set_by_label_candidates(unix, ["Unix Username"], row.get("unix_username"))

    otp = input("Enter Unix Password (OTP) and press ENTER: ").strip()
    if otp:
        set_by_label_candidates(unix, ["Unix Password"], otp)


def click_execute(page):
    candidates = [
        r"Execute Data Comparison",
        r"Execute Data Load",
        r"Execute Schema",
        r"Execute File Load",
        r"Execute File Download",
        r"Execute",
    ]
    for c in candidates:
        btn = page.get_by_role("button", name=re.compile(c, re.I))
        if btn.count() == 0:
            continue
        bv = pick_first_visible(btn)
        if is_visible(bv):
            scroll_into_view(bv)
            bv.click()
            return True
    return False


# =========================
# MAIN runner
# =========================
def run_tab_once(page, tab_key: str, df: pd.DataFrame):
    click_tab(page, tab_key)
    row = df.iloc[0].to_dict()

    print("Loaded sheet:", SHEETS[tab_key])
    print("Columns:", list(df.columns))

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

    fill_unix_block(page, row)

    if not click_execute(page):
        raise RuntimeError("Execute button not found/visible. Check UI button label.")

    print("▶ Execution started... waiting for completion signal...")
    status = wait_until_completed(page, timeout_sec=240)
    print(f"✔ Completion watcher result: {status}")


def main():
    tab_key = input(
        "Enter tab key (data_comparison/data_load/schema_generation/file_load/file_download/mismatch_explorer): "
    ).strip().lower()

    if tab_key not in SHEETS:
        raise ValueError(f"Invalid tab key: {tab_key}. Options: {list(SHEETS.keys())}")

    browser_name = input("Browser (chrome/msedge) [default=chrome]: ").strip() or "chrome"

    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEETS[tab_key])
    if df.empty:
        raise ValueError(f"No rows found in sheet: {SHEETS[tab_key]}")

    with sync_playwright() as p:
        browser = launch_browser(p, browser_name, HEADLESS)
        context = browser.new_context(viewport=None)
        page = context.new_page()
        page.set_default_timeout(DEFAULT_TIMEOUT_MS)

        page.goto(APP_URL, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(600)

        run_tab_once(page, tab_key, df)

        input("Execution completed (or timed out). Press ENTER to close browser...")
        browser.close()


if __name__ == "__main__":
    main()
