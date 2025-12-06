import requests
import pandas as pd
import json
import time
import os
import re
from typing import List, Dict, Any, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# =================== CONFIG ===================
# NOTE: ممكن تتغطى من Secrets في GitHub Actions (override تحت)
API_TOKEN =  "sonuwlfuefnrt5be8ti99puw5qc7yt7qe0dqg7gs"
API_PUBLISHER = "TheProf"

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
OUTPUT_DIR = "exports"

# ✅ هنوقف عند الصفحة 5000 كحد أقصى (أو عند آخر صفحة متاحة إن كانت أقل)
TARGET_LAST_PAGE = 5000

# ✅ عدد السجلات المطلوب جمعها في كل Run
RECORDS_PER_RUN = 5000
# =====================================

# ==== Allow env override from CI Secrets ====
API_TOKEN = os.getenv("API_TOKEN", API_TOKEN)
API_PUBLISHER = os.getenv("API_PUBLISHER", API_PUBLISHER)
HEADERS["token"] = API_TOKEN
HEADERS["publisher"] = API_PUBLISHER
# ============================================

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
                # لو مفيش ID صالح، خليه يدخل كصف “مشروح” برضه
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

    # لو زاد عدد العناصر عن المطلوب (بسبب آخر صفحة)، قصّه لـ target_records بالظبط
    if len(all_details) > target_records:
        all_details = all_details[:target_records]

    print(f"\n✅ Collected {len(all_details)} records in this run.")
    return all_details, last_page

# ============ STATE HELPERS ============
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    # مش بنعدّ سجلات تراكمية دلوقتي؛ التركيز على تقدّم الصفحات
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
    print("--- ApplicantStack Chunk Runner (every 15 min) ---")

    if not API_TOKEN or not API_PUBLISHER:
        print("!!! Please set API_TOKEN & API_PUBLISHER.")
        return

    state = load_state()
    if state.get("completed"):
        print("✅ Target last page reached earlier. Nothing to do.")
        return

    # اجمع إجمالي الصفحات أول مرة فقط
    if not state.get("total_pages"):
        state["total_pages"] = get_total_pages()
        save_state(state)

    total_pages = state["total_pages"]
    current_page = state.get("current_page", 1)

    # هنقف عند أصغر رقم بين: آخر صفحة متاحة، و TARGET_LAST_PAGE
    max_page = min(total_pages, TARGET_LAST_PAGE)

    # لو عدّينا الحد—اقفل
    if current_page > max_page:
        print(f"✅ Reached last page limit: {max_page}. Stopping.")
        state["completed"] = True
        save_state(state)
        return

    print(f"Collecting up to {RECORDS_PER_RUN} records in this run (pages {current_page}..{max_page})")

    # ⭐ اجمع لحد 5000 سجل أو نهاية الصفحات
    batch, last_page = collect_candidates_until(
        target_records=RECORDS_PER_RUN,
        start_page=current_page,
        max_page=max_page
    )

    # ملف Excel جديد للداتا الحالية فقط
    _ = save_run_to_new_excel(batch)

    # حدّث الحالة: نتقدم بالصفحات فقط
    next_page = (last_page + 1) if last_page >= current_page else current_page
    state["current_page"] = next_page
    if state["current_page"] > max_page:
        state["completed"] = True
        print(f"✅ Target last page reached: {max_page}.")

    save_state(state)
    print(f"Progress: next_page={state['current_page']} / limit_page={max_page}")

if __name__ == "__main__":
    main()
