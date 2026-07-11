import requests
import pandas as pd
import json
import time
import os
import re
from typing import List, Dict, Any, Union, Tuple, Set, Optional
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =================== CONFIG ===================
# NOTE: ممكن تتغطى من Secrets في GitHub Actions (override تحت)
API_TOKEN = ""
API_PUBLISHER = ""

BASE_URL = "https://theprofessionals.applicantstack.com/api"
CANDIDATES_LIST_URL = f"{BASE_URL}/candidates"
CANDIDATE_DETAIL_URL = f"{BASE_URL}/candidate"

HEADERS = {
    "token": API_TOKEN,
    "publisher": API_PUBLISHER,
    "Content-Type": "application/json"
}

DEFAULT_PAGE_SIZE = 100
API_CALL_DELAY = 1
MAX_RETRIES = 3
# ⭐ يمكن تغييره عبر متغير بيئة، الافتراضي 8 لتسريع الفetch للتفاصيل
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "8"))

# ====== State / Run settings ======
STATE_FILE = "applicantstack_state.json"
SEEN_IDS_FILE = "applicantstack_seen_ids.txt"
OUTPUT_DIR = "exports"

# Tail/cursor incremental settings. Each run processes a safe page batch and
# continues automatically from the saved cursor on the next run.
MAX_INCREMENTAL_PAGES = int(os.getenv("MAX_INCREMENTAL_PAGES", "100"))

# ✅ هنوقف عند الصفحة 5000 كحد أقصى (أو عند آخر صفحة متاحة إن كانت أقل)
TARGET_LAST_PAGE = 5000

# ✅ عدد السجلات المطلوب جمعها في كل Run (لو شغال بوضع الـstate)
RECORDS_PER_RUN = 50000
# =====================================

# ==== Allow env override from CI Secrets ====
API_TOKEN = os.getenv("API_TOKEN", API_TOKEN)
API_PUBLISHER = os.getenv("API_PUBLISHER", API_PUBLISHER)
HEADERS["token"] = API_TOKEN
HEADERS["publisher"] = API_PUBLISHER
# ============================================

def scrape_pages_range(start_page: int, end_page: int) -> Tuple[List[Dict[str, Any]], int]:
    all_details: List[Dict[str, Any]] = []
    last_page = start_page - 1
    for page in range(start_page, end_page + 1):
        print(f"Processing page {page}...")
        page_candidates = fetch_page_candidates(page)
        last_page = page
        if not page_candidates:
            print("  -> No candidates or fetch failed for this page. Continue.")
            continue

        candidate_ids: List[str] = []
        for summary in page_candidates:
            cid = None
            if isinstance(summary, dict):
                cid = summary.get("Candidate Serial") or summary.get("id") or summary.get("candidate_id")
            if isinstance(cid, (str, int)) and str(cid).strip():
                candidate_ids.append(str(cid))
            else:
                all_details.append({"summary_error": "Valid ID not found in summary", **summary})

        print(f"  -> Fetching details for {len(candidate_ids)} candidates concurrently (workers={MAX_WORKERS})...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            fut = {ex.submit(fetch_candidate_detail, x): x for x in candidate_ids}
            for f in as_completed(fut):
                try:
                    all_details.append(f.result())
                except Exception as exc:
                    print(f"  -> Detail fetch exception: {exc}")

    print(f"\n✅ Collected {len(all_details)} records from pages {start_page}..{end_page}.")
    return all_details, last_page

def clean_excel_name(name: str) -> str:
    invalid_chars = r'[\\/?"*:[\]]'
    name = re.sub(invalid_chars, '_', str(name))
    name = name[:31]
    return name.strip()

def robust_api_call(url: str, headers: Dict[str, str], method: str = 'GET', max_retries: int = MAX_RETRIES) -> Union[requests.Response, None]:
    for attempt in range(max_retries):
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, timeout=30)
            else:
                raise NotImplementedError("Only GET method is supported.")
            if response.status_code in [429, 500, 502, 503, 504]:
                raise requests.exceptions.HTTPError(f"Status code {response.status_code} received. Retrying...")
            response.raise_for_status()
            time.sleep(API_CALL_DELAY)
            return response
        except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** (attempt + 1)
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
            else:
                print(f"Max retries ({max_retries}) reached for {url}. Giving up.")
                return None
    return None

def fetch_page_candidates(page_number: int) -> Union[List[Dict[str, Any]], None]:
    url = f"{CANDIDATES_LIST_URL}/{page_number}"
    response = robust_api_call(url, HEADERS)
    if response is None:
        return None
    try:
        response.encoding = 'utf-8'
        candidates_data = response.json()
        candidate_list = []
        if isinstance(candidates_data, list):
            candidate_list = candidates_data
        elif isinstance(candidates_data, dict):
            # التكيّف مع تنسيقات مختلفة من الAPI
            for value in candidates_data.values():
                if isinstance(value, list) and len(value) > len(candidate_list):
                    candidate_list = value
            if not candidate_list and candidates_data.get('error'):
                print(f"API Error on page {page_number}: {candidates_data.get('error')}")
                return None
        else:
            print(f"Error: Unexpected response type for page {page_number}: {type(candidates_data)}. Content: {response.text[:200]}...")
            return None
        if not candidate_list:
            return None
        return candidate_list
    except json.JSONDecodeError:
        print(f"Failed to decode JSON response for page {page_number}. Response: {response.text[:200]}...")
        return None

def fetch_candidate_detail(candidate_id: str) -> Dict[str, Any]:
    url = f"{CANDIDATE_DETAIL_URL}/{candidate_id}"
    response = robust_api_call(url, HEADERS)
    if response is None:
        return {"Candidate Serial": candidate_id, "detail_fetch_error": "Failed after max retries."}
    try:
        response.encoding = 'utf-8'
        detail = response.json()
        flat_detail = flatten_questionnaires(detail)
        flat_detail = flatten_history_data(flat_detail, 'Job Submissions', 'Job Submissions')
        flat_detail = flatten_history_data(flat_detail, 'Application History', 'Application History')
        return flat_detail
    except json.JSONDecodeError:
        return {"Candidate Serial": candidate_id, "detail_fetch_error": "JSON Decode Error"}

def flatten_questionnaires(candidate_detail: Dict[str, Any]) -> Dict[str, Any]:
    flat_detail = candidate_detail.copy()
    questionnaires = flat_detail.pop('Questionnaires', [])
    if not questionnaires:
        return flat_detail
    for i, questionnaire in enumerate(questionnaires):
        q_name = questionnaire.get('Questionnaire Name', f'Questionnaire_{i+1}')
        flat_detail[f'{q_name} - Serial'] = questionnaire.get('Questionnaire Serial', '')
        flat_detail[f'{q_name} - Submit Date'] = questionnaire.get('Submit Date')
        questions = questionnaire.get('Questions', [])
        for question_item in questions:
            question = question_item.get('Question')
            value = question_item.get('Value')
            if question and value is not None:
                flat_detail[question] = value
    return flat_detail

def flatten_history_data(candidate_detail: Dict[str, Any], key: str, prefix: str) -> Dict[str, Any]:
    flat_detail = candidate_detail.copy()
    history_list = flat_detail.pop(key, [])
    if not history_list:
        flat_detail[f'{prefix} Summary'] = 'No records found'
        return flat_detail
    summary_parts = []
    for item in history_list:
        job_name = item.get('Job Name') or item.get('Job Title') or 'N/A'
        date = item.get('Date') or item.get('Create Date') or 'N/A'
        status = item.get('Status') or item.get('Stage') or 'N/A'
        summary_parts.append(f"[{job_name} | {date} | {status}]")
    flat_detail[f'{prefix} Summary'] = ' || '.join(summary_parts)
    flat_detail[f'{prefix} Count'] = len(history_list)
    return flat_detail

def get_total_pages() -> int:
    print("Attempting to determine total number of pages using base API...")
    url = CANDIDATES_LIST_URL + "/"
    response = robust_api_call(url, HEADERS)
    if response is None:
        print("Failed to get total pages after max retries. Assuming 1 page.")
        return 1
    try:
        response.encoding = 'utf-8'
        metadata = response.json()
        total_pages = metadata.get('NumPages')
        total_candidates = metadata.get('TotalCount')
        try:
            total_pages = int(total_pages)
            total_candidates = int(total_candidates)
        except (ValueError, TypeError):
            print("Could not find a valid 'NumPages' or 'TotalCount'. Assuming 1 page.")
            return 1
        print(f"Total Candidates: {total_candidates}.")
        print(f"Total Pages Available: {total_pages}")
        return total_pages
    except json.JSONDecodeError:
        print("Failed to decode JSON response from base API. Assuming 1 page.")
        return 1

def collect_candidates_until(target_records: int, start_page: int, max_page: int) -> Tuple[List[Dict[str, Any]], int]:
    """
    يلفّ على الصفحات من start_page حتى max_page ويجمع تفاصيل المرشحين
    لحدّ ما يوصل target_records أو تخلص الصفحات.
    يرجّع (البيانات, آخر صفحة تم الوصول لها).
    """
    all_details: List[Dict[str, Any]] = []
    last_page = start_page - 1
    page = start_page

    while len(all_details) < target_records and page <= max_page:
        print(f"Processing page {page}...")
        page_candidates = fetch_page_candidates(page)
        last_page = page
        page += 1

        if not page_candidates:
            print("  -> No candidates or fetch failed for this page. Continue.")
            continue

        candidate_ids: List[str] = []
        for summary in page_candidates:
            candidate_id = None
            if isinstance(summary, dict):
                candidate_id = summary.get("Candidate Serial") or summary.get("id") or summary.get("candidate_id")
            if isinstance(candidate_id, (str, int)) and str(candidate_id).strip():
                candidate_ids.append(str(candidate_id))
            else:
                all_details.append({"summary_error": "Valid ID not found in summary", **summary})

        print(f"  -> Fetching details for {len(candidate_ids)} candidates concurrently (workers={MAX_WORKERS})...")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_id = {executor.submit(fetch_candidate_detail, cid): cid for cid in candidate_ids}
            for future in as_completed(future_to_id):
                try:
                    detail = future.result()
                    all_details.append(detail)
                    if len(all_details) >= target_records:
                        break
                except Exception as exc:
                    cid = future_to_id[future]
                    print(f"  -> Detail fetch for Candidate {cid} generated an exception: {exc}")

    if len(all_details) > target_records:
        all_details = all_details[:target_records]

    print(f"\n✅ Collected {len(all_details)} records in this run.")
    return all_details, last_page

# ============ INCREMENTAL SYNC HELPERS ============
def extract_candidate_id(item: Dict[str, Any]) -> Optional[str]:
    if not isinstance(item, dict):
        return None
    value = item.get("Candidate Serial") or item.get("id") or item.get("candidate_id")
    if isinstance(value, (str, int)) and str(value).strip():
        return str(value).strip()
    return None


def load_seen_ids() -> Set[str]:
    """Load the lightweight persistent set of candidate IDs."""
    path = Path(SEEN_IDS_FILE)
    if not path.exists():
        return set()
    return {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }


def save_seen_ids(seen_ids: Set[str]) -> None:
    """Write IDs atomically so an interrupted run cannot corrupt the checkpoint."""
    path = Path(SEEN_IDS_FILE)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text("\n".join(sorted(seen_ids)) + "\n", encoding="utf-8")
    temp_path.replace(path)


def bootstrap_seen_ids_from_exports() -> Set[str]:
    """
    One-time migration: build the ID checkpoint from existing Excel exports.
    Later runs load the text file directly and do not scan Excel files again.
    """
    seen_ids: Set[str] = set()
    export_files = sorted(Path(OUTPUT_DIR).glob("applicantstack_*.xlsx"))
    if not export_files:
        print("No existing exports found; starting with an empty seen-ID set.")
        return seen_ids

    print(f"Bootstrapping seen IDs from {len(export_files)} existing Excel file(s)...")
    for excel_file in export_files:
        try:
            header = pd.read_excel(excel_file, nrows=0)
            candidate_column = next(
                (col for col in header.columns if str(col).strip().lower() == "candidate serial"),
                None,
            )
            if candidate_column is None:
                print(f"  -> Skipping {excel_file.name}: Candidate Serial column not found.")
                continue
            frame = pd.read_excel(excel_file, usecols=[candidate_column])
            ids = (
                frame[candidate_column]
                .dropna()
                .astype(str)
                .str.strip()
            )
            # Excel sometimes converts integer-looking IDs to values ending in .0.
            ids = ids.str.replace(r"^([0-9]+)\.0$", r"\1", regex=True)
            seen_ids.update(value for value in ids if value)
            print(f"  -> {excel_file.name}: checkpoint now has {len(seen_ids)} IDs.")
        except Exception as exc:
            print(f"  -> Could not read {excel_file.name}: {exc}")

    if seen_ids:
        save_seen_ids(seen_ids)
        print(f"✅ Created {SEEN_IDS_FILE} with {len(seen_ids)} unique IDs.")
    return seen_ids


def ensure_seen_ids() -> Set[str]:
    seen_ids = load_seen_ids()
    if seen_ids:
        print(f"Loaded {len(seen_ids)} previously exported candidate IDs.")
        return seen_ids
    return bootstrap_seen_ids_from_exports()


def fetch_new_candidate_details(candidate_ids: List[str]) -> Tuple[List[Dict[str, Any]], Set[str], List[str]]:
    """Fetch only unseen candidates. Failed IDs are deliberately not checkpointed."""
    details: List[Dict[str, Any]] = []
    successful_ids: Set[str] = set()
    failed_ids: List[str] = []

    if not candidate_ids:
        return details, successful_ids, failed_ids

    print(f"  -> Fetching details for {len(candidate_ids)} NEW candidates (workers={MAX_WORKERS})...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_id = {
            executor.submit(fetch_candidate_detail, candidate_id): candidate_id
            for candidate_id in candidate_ids
        }
        for future in as_completed(future_to_id):
            candidate_id = future_to_id[future]
            try:
                detail = future.result()
                if not isinstance(detail, dict) or detail.get("detail_fetch_error"):
                    failed_ids.append(candidate_id)
                    print(f"  -> Candidate {candidate_id} detail failed; it will be retried next run.")
                    continue
                details.append(detail)
                successful_ids.add(candidate_id)
            except Exception as exc:
                failed_ids.append(candidate_id)
                print(f"  -> Candidate {candidate_id} detail exception: {exc}")

    return details, successful_ids, failed_ids


def collect_incremental_candidates(
    seen_ids: Set[str],
    total_pages: int,
    resume_page: int,
) -> Tuple[List[Dict[str, Any]], Set[str], Dict[str, Any]]:
    """
    Tail/cursor incremental sync for this ApplicantStack account.

    The API is ordered from older pages to newer pages, so new candidates are
    appended near the end. We therefore continue from the last unprocessed
    page, with a small overlap to catch a partially-filled page or pagination
    movement. Candidate Serial remains the final deduplication key.
    """
    all_details: List[Dict[str, Any]] = []
    successful_new_ids: Set[str] = set()
    failed_pages: List[int] = []
    failed_candidate_ids: List[str] = []
    pages_checked_count = 0
    last_page_checked = max(0, resume_page - 1)
    run_seen_ids = set(seen_ids)

    overlap_pages = int(os.getenv("INCREMENTAL_OVERLAP_PAGES", "2"))
    resume_page = max(1, resume_page)
    scan_start = max(1, resume_page - overlap_pages)
    page_limit = min(total_pages, resume_page + MAX_INCREMENTAL_PAGES - 1)

    # If the stored cursor is already beyond today's final page, only recheck
    # the tail overlap. This catches new rows added to the current last page.
    if resume_page > total_pages:
        scan_start = max(1, total_pages - overlap_pages + 1)
        page_limit = total_pages

    print(
        f"Tail incremental scan: cursor={resume_page}, "
        f"checking pages {scan_start}..{page_limit} of {total_pages}."
    )

    for page in range(scan_start, page_limit + 1):
        print(f"Checking incremental page {page}/{total_pages}...")
        summaries = fetch_page_candidates(page)
        pages_checked_count += 1
        last_page_checked = page

        if summaries is None:
            failed_pages.append(page)
            print("  -> Page failed or returned no usable list; it will be retried.")
            continue

        page_ids: List[str] = []
        for summary in summaries:
            candidate_id = extract_candidate_id(summary)
            if candidate_id:
                page_ids.append(candidate_id)

        page_ids = list(dict.fromkeys(page_ids))
        new_ids = [candidate_id for candidate_id in page_ids if candidate_id not in run_seen_ids]
        print(f"  -> {len(new_ids)} new / {len(page_ids)} total IDs on page {page}.")

        if new_ids:
            details, successful_ids, failed_ids = fetch_new_candidate_details(new_ids)
            all_details.extend(details)
            successful_new_ids.update(successful_ids)
            failed_candidate_ids.extend(failed_ids)
            run_seen_ids.update(successful_ids)

    # Never advance past a failed list page; retry it on the next run.
    if failed_pages:
        next_scan_page = min(failed_pages)
        backlog_incomplete = True
        stop_reason = f"retry failed page {next_scan_page}"
    elif page_limit < total_pages:
        next_scan_page = page_limit + 1
        backlog_incomplete = True
        stop_reason = f"batch page limit reached ({MAX_INCREMENTAL_PAGES})"
    else:
        # Cursor points immediately after the latest page. Future runs recheck
        # the tail overlap and continue automatically when total_pages grows.
        next_scan_page = total_pages + 1
        backlog_incomplete = False
        stop_reason = "caught up to current last page"

    summary = {
        "mode": "tail_incremental",
        "scan_start_page": scan_start,
        "last_page_checked": last_page_checked,
        "pages_checked_count": pages_checked_count,
        "new_candidates_found": len(successful_new_ids) + len(set(failed_candidate_ids)),
        "new_candidates_saved": len(successful_new_ids),
        "failed_pages": failed_pages,
        "failed_candidate_ids": sorted(set(failed_candidate_ids)),
        "stop_reason": stop_reason,
        "backlog_incomplete": backlog_incomplete,
        "next_scan_page": next_scan_page,
    }
    return all_details, successful_new_ids, summary


# ============ STATE HELPERS ============
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"current_page": 1, "total_pages": None, "completed": False}

def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
# ======================================

def save_run_to_new_excel(data: List[Dict[str, Any]]) -> int:
    """يحفظ داتا الRun الحالية فقط في ملف Excel جديد باسم timestamp داخل مجلد exports/"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if not data:
        print("No data for this run. Skipping Excel file creation.")
        return 0

    df = pd.DataFrame(data)
    cleaned_columns = {col: clean_excel_name(col) for col in df.columns}
    df.rename(columns=cleaned_columns, inplace=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")  # UTC timestamp
    filename = os.path.join(OUTPUT_DIR, f"applicantstack_{ts}.xlsx")

    try:
        df.to_excel(filename, index=False, sheet_name='Sheet1')
        print(f"🧾 Created new Excel file for this run: {filename} (+{len(df)} rows)")
        return len(df)
    except Exception as e:
        print(f"🚨 Excel save error: {e}")
        print("Ensure 'openpyxl' is installed")
        return 0

def main():
    print("--- ApplicantStack Smart Incremental Runner ---")

    if not API_TOKEN or not API_PUBLISHER:
        print("!!! Missing API_TOKEN or API_PUBLISHER. Configure GitHub repository secrets.")
        return

    run_mode = os.getenv("RUN_MODE", "incremental").strip().lower()

    # Manual range remains available for recovery/backfill jobs only.
    if run_mode == "manual_range":
        sp_env = os.getenv("START_PAGE")
        ep_env = os.getenv("END_PAGE")
        if not sp_env or not ep_env:
            print("Manual range mode requires START_PAGE and END_PAGE.")
            return
        try:
            sp = max(1, int(sp_env))
            ep = max(sp, int(ep_env))
        except ValueError:
            print("Invalid START_PAGE/END_PAGE.")
            return

        total_pages = get_total_pages()
        ep = min(ep, total_pages)
        if sp > total_pages:
            print(f"Start page {sp} > total pages {total_pages}. Nothing to do.")
            return
        print(f"Manual range mode: pages {sp}..{ep}")
        batch, last_page = scrape_pages_range(sp, ep)
        saved_rows = save_run_to_new_excel(batch)
        save_state({
            "mode": "manual_range",
            "start_page": sp,
            "end_page": ep,
            "last_page_attempted": last_page,
            "records_saved": saved_rows,
            "last_run_utc": datetime.utcnow().isoformat() + "Z",
        })
        return

    # Default and scheduled mode: tail/cursor incremental sync.
    seen_ids = ensure_seen_ids()
    previous_state = load_state()
    # The previous export stopped after page 5000. The state file supplied with
    # this update starts at 5001; after that the cursor is maintained automatically.
    resume_page = int(previous_state.get("next_scan_page", 5001))
    total_pages = get_total_pages()
    details, successful_new_ids, run_summary = collect_incremental_candidates(
        seen_ids=seen_ids,
        total_pages=total_pages,
        resume_page=resume_page,
    )

    saved_rows = save_run_to_new_excel(details)

    # Only checkpoint IDs after their details were fetched AND the Excel was saved.
    if saved_rows > 0:
        seen_ids.update(successful_new_ids)
        save_seen_ids(seen_ids)
        print(f"✅ Checkpoint updated: {len(seen_ids)} total exported candidate IDs.")
    elif successful_new_ids:
        print("⚠️ Excel was not saved, so new IDs were NOT checkpointed and will be retried.")

    run_summary.update({
        "total_pages_at_run": total_pages,
        "records_saved": saved_rows,
        "seen_ids_total": len(seen_ids),
        "last_run_utc": datetime.utcnow().isoformat() + "Z",
    })
    save_state(run_summary)

    if saved_rows == 0:
        print("✅ No new ApplicantStack candidates found.")
    print(f"Run summary: {json.dumps(run_summary, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
