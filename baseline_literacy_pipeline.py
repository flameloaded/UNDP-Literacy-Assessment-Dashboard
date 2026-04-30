import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

BASE_URL = os.getenv("MOODLE_BASE_URL")
TOKEN = os.getenv("MOODLE_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

TIMEOUT = 60
REQUEST_DELAY = 0.08

INCLUDE_COURSE_IDS = [16]

OUTPUT_FILE = "data/baseline_literacy_responses.csv"
BACKUP_FILE = "data/baseline_literacy_responses_backup.csv"
TEMP_FILE = "data/temp_baseline_literacy_responses.csv"


def call_moodle(wsfunction, **kwargs):
    params = {
        "wstoken": TOKEN,
        "moodlewsrestformat": "json",
        "wsfunction": wsfunction,
    }
    params.update(kwargs)

    response = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()

    data = response.json()

    if isinstance(data, dict) and data.get("exception"):
        raise Exception(
            f"{wsfunction} failed: {data.get('message')} | {data.get('debuginfo', '')}"
        )

    time.sleep(REQUEST_DELAY)
    return data


def safe_call(wsfunction, **kwargs):
    try:
        return call_moodle(wsfunction, **kwargs), None
    except Exception as e:
        return None, str(e)


def fetch_courses():
    courses = call_moodle("core_course_get_courses")
    courses_df = pd.DataFrame(courses)

    courses_df = courses_df[courses_df["id"] != 1].copy()

    courses_df = courses_df.rename(columns={
        "id": "course_id",
        "fullname": "course_name",
        "shortname": "course_shortname",
    })

    courses_df["course_id"] = pd.to_numeric(courses_df["course_id"], errors="coerce")

    courses_df = courses_df[
        courses_df["course_id"].isin(INCLUDE_COURSE_IDS)
    ].copy()

    return courses_df


def fetch_users(courses_df):
    rows = []

    for _, row in courses_df.iterrows():
        course_id = int(row["course_id"])
        course_name = row["course_name"]

        users_data, err = safe_call(
            "core_enrol_get_enrolled_users",
            courseid=course_id
        )

        if err:
            print(f"Could not fetch users for course {course_id}: {err}", flush=True)
            continue

        if not isinstance(users_data, list):
            continue

        for user in users_data:
            rows.append({
                "course_id": course_id,
                "course_name": course_name,
                "user_id": user.get("id"),
                "fullname": user.get("fullname"),
                "email": user.get("email"),
                "username": user.get("username"),
                "suspended": user.get("suspended"),
            })

    return pd.DataFrame(rows).drop_duplicates(
        subset=["course_id", "user_id"]
    ).copy()


def fetch_feedbacks(courses_df):
    course_ids = courses_df["course_id"].dropna().astype(int).tolist()

    params = {
        "wstoken": TOKEN,
        "moodlewsrestformat": "json",
        "wsfunction": "mod_feedback_get_feedbacks_by_courses",
    }

    for i, cid in enumerate(course_ids):
        params[f"courseids[{i}]"] = int(cid)

    response = requests.get(BASE_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()

    payload = response.json()

    if isinstance(payload, dict) and payload.get("exception"):
        raise Exception(
            f"mod_feedback_get_feedbacks_by_courses failed: "
            f"{payload.get('message')} | {payload.get('debuginfo', '')}"
        )

    feedbacks = payload.get("feedbacks", [])

    feedbacks_df = pd.DataFrame([{
        "course_id": f.get("course"),
        "feedback_id": f.get("id"),
        "cmid": f.get("cmid"),
        "feedback_name": f.get("name"),
        "intro": f.get("intro"),
        "anonymous": f.get("anonymous"),
        "multiple_submit": f.get("multiple_submit"),
        "autonumbering": f.get("autonumbering"),
        "time_open": f.get("timeopen"),
        "time_close": f.get("timeclose"),
    } for f in feedbacks])

    if feedbacks_df.empty:
        return feedbacks_df

    feedbacks_df = feedbacks_df.merge(
        courses_df[["course_id", "course_name"]],
        on="course_id",
        how="left"
    )

    return feedbacks_df


def fetch_feedback_responses(feedback_row):
    rows = []

    feedback_id = int(feedback_row["feedback_id"])
    course_id = int(feedback_row["course_id"])
    course_name = feedback_row["course_name"]
    feedback_name = feedback_row["feedback_name"]

    resp_data, resp_err = safe_call(
        "mod_feedback_get_responses_analysis",
        feedbackid=feedback_id,
        page=0,
        perpage=1000
    )

    if resp_err:
        print(f"Error for feedback {feedback_id}: {resp_err}", flush=True)
        return pd.DataFrame()

    attempts = resp_data.get("attempts", []) if isinstance(resp_data, dict) else []

    for attempt in attempts:
        user_id = attempt.get("userid")
        fullname = attempt.get("fullname")
        timemodified = attempt.get("timemodified")

        for response in attempt.get("responses", []):
            rows.append({
                "course_id": course_id,
                "course_name": course_name,
                "feedback_id": feedback_id,
                "feedback_name": feedback_name,
                "user_id": user_id,
                "fullname": fullname,
                "timemodified": timemodified,
                "item_id": response.get("id"),
                "question": response.get("name"),
                "answer": response.get("printval"),
                "raw_response": response.get("rawval"),
            })

    return pd.DataFrame(rows)


def apply_question_mapping(df):
    question_map = {
        "Data Privacy & Consent: ...":
            "Data Privacy & Consent: Do you consent to these terms?",

        "Have you used any digital ...":
            "Have you used any digital skills to solve a problem or improve a process at your host organization?",

        "How comfortable are you ...":
            "How comfortable are you using AI tools (like ChatGPT, Gemini, Copilot) to carry out work tasks?",

        "How confident are you using...":
            "How confident are you using digital tools (e.g., email, shared documents, spreadsheets) to complete your work tasks independently?",

        "If you answered 'Yes' to ...":
            "If you answered 'Yes' to the previous question, please briefly describe one example.",

        'If you pick "Other" above ...':
            'If you picked "Other" above, please specify.',

        'If you picked "Other" for ...':
            'If you picked "Other" for the question above, please specify.',

        "If your answer above is ...":
            "If your answer above is Other, please specify.",

        "Rate your understanding of ...":
            "Rate your understanding of Data Privacy (e.g., protecting customer information, password security, or GDPR/NDPR basics).",

        "Select your geo-political ...":
            "Select your geo-political zone.",

        "What device(s) do you ...":
            "What device(s) do you primarily use to access digital learning and work tools?",

        "What digital tasks do you ...":
            "What digital tasks do you find most difficult at work?",

        "What digital tasks does ...":
            "What digital tasks does your current role at your host organization require you to perform?",

        "What is your preferred mode...":
            "What is your preferred mode of learning?",

        "Which of these emerging ...":
            "Which of these emerging technologies interests you the most?",

        "Which of these would most ...":
            "Which of these would most likely improve your productivity in your current role?",

        "Which productivity tools do...":
            "Which productivity tools do you currently use regularly in your work? (Select all that apply)",

        "Which statement best ...":
            "Which statement best describes how the knowledge of technology tools affect your job performance?",
    }

    df = df.copy()
    df["question_full"] = df["question"].replace(question_map)
    return df


def build_dataset():
    print("Fetching courses...", flush=True)
    courses_df = fetch_courses()

    if courses_df.empty:
        raise ValueError("No selected course found.")

    print("Fetching users...", flush=True)
    users_df = fetch_users(courses_df)

    print("Fetching feedback activities...", flush=True)
    feedbacks_df = fetch_feedbacks(courses_df)

    if feedbacks_df.empty:
        raise ValueError("No feedback activities found.")

    all_responses = []

    print("Fetching feedback responses...", flush=True)
    for _, feedback_row in feedbacks_df.iterrows():
        resp_df = fetch_feedback_responses(feedback_row)
        if not resp_df.empty:
            all_responses.append(resp_df)

    feedback_responses_df = (
        pd.concat(all_responses, ignore_index=True)
        if all_responses else pd.DataFrame()
    )

    if feedback_responses_df.empty:
        raise ValueError("No LMS responses found.")

    print("Adding email to responses...", flush=True)
    feedback_responses_df = feedback_responses_df.merge(
        users_df[["user_id", "email", "username", "suspended"]],
        on="user_id",
        how="left"
    )

    feedback_responses_df["email_clean"] = (
        feedback_responses_df["email"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    feedback_responses_df = apply_question_mapping(feedback_responses_df)

    return feedback_responses_df

def get_database_engine():
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL is missing. Add it to your environment variables.")

    db_url = DATABASE_URL.strip()

    # SQLAlchemy works best with postgresql:// or postgresql+psycopg2://
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    elif db_url.startswith("postgresql://"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg2://", 1)

    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
    )

# Connect to database

def save_to_supabase(df):
    table_name = "baseline_literacy_responses"
    temp_table = "baseline_literacy_responses_temp"
    backup_table = "baseline_literacy_responses_backup"

    if df.empty:
        raise ValueError("Dataframe is empty. Nothing to save to Supabase.")

    print("Saving dataset to Supabase...", flush=True)

    engine = get_database_engine()

    # Replace NaN values with None so PostgreSQL stores them as NULL
    df_to_save = df.copy()
    df_to_save = df_to_save.where(pd.notnull(df_to_save), None)

    # Drop old temp table first
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{temp_table}"'))

    # Write fresh data into a temp table first
    df_to_save.to_sql(
        temp_table,
        engine,
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi",
    )

    # Swap temp table into main table safely
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{backup_table}"'))
        conn.execute(text(f'ALTER TABLE IF EXISTS "{table_name}" RENAME TO "{backup_table}"'))
        conn.execute(text(f'ALTER TABLE "{temp_table}" RENAME TO "{table_name}"'))

        conn.execute(text(
            f'CREATE INDEX IF NOT EXISTS idx_{table_name}_course_id '
            f'ON "{table_name}" (course_id)'
        ))

        conn.execute(text(
            f'CREATE INDEX IF NOT EXISTS idx_{table_name}_user_id '
            f'ON "{table_name}" (user_id)'
        ))

        conn.execute(text(
            f'CREATE INDEX IF NOT EXISTS idx_{table_name}_question_full '
            f'ON "{table_name}" (question_full)'
        ))

    print("✅ Dataset saved to Supabase successfully.", flush=True)


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    try:
        df = build_dataset()

        if df.empty:
            raise ValueError("New dataset is empty. Aborting update.")

        # Save to Supabase database
        save_to_supabase(df)

        # Optional: still save local CSV backup
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8", na_rep="")

        print("✅ Baseline literacy raw LMS responses updated successfully", flush=True)
        print(f"Rows saved: {len(df)}", flush=True)
        print(f"Unique users: {df['user_id'].nunique()}", flush=True)
        print(f"Unique questions: {df['question_full'].nunique()}", flush=True)

    except Exception as e:
        print("❌ Pipeline failed:", e, flush=True)
        print("⚠️ Supabase table was not updated.", flush=True)
        raise