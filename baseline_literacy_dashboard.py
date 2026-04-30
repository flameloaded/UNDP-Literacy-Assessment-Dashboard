import os
import re
import html
import pandas as pd
import streamlit as st
import plotly.express as px
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()



def get_secret_value(name):
    try:
        return st.secrets[name]
    except Exception:
        return os.getenv(name)
    
    

st.set_page_config(
    page_title="Baseline Literacy Assessment Dashboard",
    layout="wide"
)

# -----------------------------
# HELPERS
# -----------------------------
# -----------------------------
# DATABASE CONNECTION
# -----------------------------

TABLE_NAME = "baseline_literacy_responses"


@st.cache_resource
def get_database_engine():
    database_url = get_secret_value("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL is missing. Add it to your environment variables.")

    database_url = database_url.strip()

    if database_url.startswith("postgres://"):
        database_url = database_url.replace(
            "postgres://",
            "postgresql+psycopg2://",
            1
        )

    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace(
            "postgresql://",
            "postgresql+psycopg2://",
            1
        )

    return create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0
    )


@st.cache_data(ttl=600)
def load_data():
    engine = get_database_engine()

    query = text(f'SELECT * FROM "{TABLE_NAME}"')

    df = pd.read_sql_query(query, engine)

    if df.empty:
        raise ValueError("The Supabase table is empty.")

    return df


def clean_text(text):
    if pd.isna(text):
        return ""

    text = str(text)
    text = html.unescape(text)
    text = text.lower().strip()
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def categorize(text):
    text = str(text).lower()

    if any(x in text for x in ["data", "analysis", "analytics", "visualization", "power bi", "tableau"]):
        return "Data Analysis & Visualization"
    elif any(x in text for x in ["excel", "spreadsheet", "google sheet"]):
        return "Excel / Spreadsheets"
    elif any(x in text for x in ["coding", "programming", "python", "java", "code"]):
        return "Programming / Development"
    elif any(x in text for x in ["design", "canva", "photoshop", "graphics", "animation"]):
        return "Design & Creative Tools"
    elif any(x in text for x in ["data entry", "entry", "typing", "record"]):
        return "Data Entry & Admin Tasks"
    elif any(x in text for x in ["ai", "automation", "chatgpt", "copilot", "gemini"]):
        return "AI & Automation"
    elif any(x in text for x in ["project", "trello", "management", "crm"]):
        return "Project / Business Tools"
    elif any(x in text for x in ["email", "communication", "word", "office", "google workspace"]):
        return "Office & Communication Tools"
    elif any(x in text for x in ["marketing", "social media"]):
        return "Digital Marketing"
    elif any(x in text for x in ["network", "electricity", "hardware"]):
        return "Infrastructure Issues"
    elif any(x in text for x in ["learning", "not familiar", "don’t know", "don't know", "new tools"]):
        return "Learning / Skill Gap"
    elif any(x in text for x in ["none", "nil", "n/a", "nothing", "not applicable"]):
        return "No Challenge"
    else:
        return "Other"


def classify_question(n):
    if n >= 50:
        return "Open-ended"
    elif n >= 10:
        return "Semi-open / Multi-select"
    else:
        return "Structured (MCQ)"


# -----------------------------
# GROUPING RULES FOR "OTHER" QUESTIONS
# -----------------------------
OTHER_GROUPING_MAP = {
    'If you picked "Other" above, please specify.': {
        "Google Workspace": [
            "google", "google workspace", "google docs", "google sheets",
            "google slides", "docs", "sheets"
        ],
        "Microsoft Office": [
            "microsoft office", "office", "excel", "word", "powerpoint"
        ],
        "Communication Tools": [
            "email", "outlook", "slack", "teams", "communication"
        ],
        "Project Management Tools": [
            "trello", "asana", "monday", "jira", "notion", "project"
        ],
        "Design Tools": [
            "canva", "photoshop", "figma", "coreldraw", "design", "graphics"
        ],
        "Data / Reporting Tools": [
            "power bi", "tableau", "analytics", "analysis", "dashboard", "report"
        ],
        "Other": []
    },

    'If you picked "Other" for the question above, please specify.': {
        "AI & Automation": [
            "ai", "artificial intelligence", "automation", "chatgpt", "copilot", "gemini"
        ],
        "Blockchain / Web3": [
            "blockchain", "web3", "crypto"
        ],
        "AR / VR": [
            "ar", "vr", "augmented reality", "virtual reality"
        ],
        "IoT / Robotics": [
            "iot", "internet of things", "robotics", "robot"
        ],
        "Data / Analytics": [
            "data", "analytics", "machine learning", "ml"
        ],
        "Other": []
    },

    "If your answer above is Other, please specify.": {
        "Training & Capacity Building": [
            "training", "learn", "learning", "workshop", "capacity building", "skill"
        ],
        "Better Tools / Software": [
            "tool", "software", "system", "platform", "application", "app"
        ],
        "Internet / Connectivity": [
            "internet", "wifi", "network", "connectivity", "data subscription"
        ],
        "Devices / Hardware": [
            "laptop", "desktop", "computer", "phone", "tablet", "device"
        ],
        "Support / Guidance": [
            "support", "guidance", "mentorship", "assistance", "help"
        ],
        "Time / Process Improvement": [
            "time", "efficiency", "faster", "process", "workflow", "productivity"
        ],
        "Other": []
    }
}


def categorize_grouped_response(text, rules):
    text = clean_text(text)
    matches = []

    for category, keywords in rules.items():
        if category == "Other":
            continue
        if any(keyword in text for keyword in keywords):
            matches.append(category)

    return matches if matches else ["Other"]


def get_grouped_question_counts(question_df, question):
    if question not in OTHER_GROUPING_MAP:
        return pd.DataFrame(columns=["category", "count", "percentage"])

    df_group = question_df.copy()
    df_group = df_group[df_group["answer"].notna()].copy()
    df_group["answer"] = df_group["answer"].astype(str).str.strip()
    df_group = df_group[df_group["answer"] != ""]

    if df_group.empty:
        return pd.DataFrame(columns=["category", "count", "percentage"])

    rules = OTHER_GROUPING_MAP[question]
    df_group["category_list"] = df_group["answer"].apply(
        lambda x: categorize_grouped_response(x, rules)
    )
    df_group = df_group.explode("category_list")

    grouped_counts = (
        df_group["category_list"]
        .value_counts()
        .reset_index()
    )
    grouped_counts.columns = ["category", "count"]
    grouped_counts["percentage"] = (
        grouped_counts["count"] / grouped_counts["count"].sum() * 100
    ).round(2)

    return grouped_counts


# -----------------------------
# GROUPING RULES FOR "IF YES" QUESTION
# -----------------------------
IF_YES_QUESTION = "If you answered 'Yes' to the previous question, please briefly describe one example."

IF_YES_GROUPING_RULES = {
    "Excel / Spreadsheets": [
        "excel", "spreadsheet", "spreadsheets", "worksheet", "pivot table", "formula",
        "formulas", "google sheets", "sheet", "report cards", "attendance sheet",
        "calculate result", "track records", "financial reports", "complex calculation"
    ],
    "Data Analysis & Reporting": [
        "data analysis", "analysis", "analytics", "report", "reporting", "weekly performance reports",
        "visualization", "power bi", "powerbi", "dashboard", "clean data", "data processing"
    ],
    "Data Entry & Records Management": [
        "data entry", "entering data", "record", "records", "student records", "daily sales",
        "expenses", "input and save records", "input and evaluate", "collecting surveys",
        "organize students names", "organize and clean data"
    ],
    "Google Workspace & Forms": [
        "google form", "google forms", "google sheet", "google sheets", "google workspace",
        "shared document", "shared documents", "google docs"
    ],
    "AI / Automation": [
        "ai", "chatgpt", "chatgtp", "gemini", "copilot", "automation", "automate",
        "diagnostic ai", "research writing", "prompt"
    ],
    "Digital Communication": [
        "email", "emailed", "whatsapp", "facebook", "social media", "communicate",
        "mobilize people", "digital communication", "group that connected"
    ],
    "Design / Creative Work": [
        "canva", "design", "graphics", "flyer", "promotional materials", "staff id card",
        "brand", "designing a flyer"
    ],
    "Process Improvement / Digitization": [
        "improve", "improved", "faster", "efficiency", "efficient", "process", "workflow",
        "productivity", "solve", "solved", "reduced manual work", "saved time", "digitized",
        "digital management system", "paper archives", "searchable pdfs", "digital tracking",
        "reduced paperwork", "errors", "streamlined"
    ],
    "Project / Task Management": [
        "trello", "assign tasks", "developers", "track progress", "collaboration"
    ],
    "Web / System Development": [
        "web based system", "front end website", "building a front end website", "developed a web based system"
    ],
    "No Example / Not Matched": [
        "nil", "i have not been matched", "i currently dont have a host organization",
        "i used my laptop and smart phone to join the host orgnization",
        "zamara world health organization"
    ],
    "Other": []
}


def categorize_if_yes_response(text):
    text = clean_text(text)
    matches = []

    for category, keywords in IF_YES_GROUPING_RULES.items():
        if category == "Other":
            continue
        if any(keyword in text for keyword in keywords):
            matches.append(category)

    return matches if matches else ["Other"]


def get_if_yes_grouped_counts(question_df):
    df_group = question_df.copy()
    df_group = df_group[df_group["answer"].notna()].copy()
    df_group["answer"] = df_group["answer"].astype(str).str.strip()
    df_group = df_group[df_group["answer"] != ""]

    if df_group.empty:
        return pd.DataFrame(columns=["category", "count", "percentage"])

    df_group["category_list"] = df_group["answer"].apply(categorize_if_yes_response)
    df_group = df_group.explode("category_list")

    grouped_counts = (
        df_group["category_list"]
        .value_counts()
        .reset_index()
    )
    grouped_counts.columns = ["category", "count"]
    grouped_counts["percentage"] = (
        grouped_counts["count"] / grouped_counts["count"].sum() * 100
    ).round(2)

    return grouped_counts


def prep_data(df):
    df = df.copy()

    if "category" not in df.columns:
        df["category"] = None

    target_questions = [
        "What digital tasks do you find most difficult at work?",
        "Which of these emerging technologies interests you the most?"
    ]

    if "question_full" in df.columns and "answer" in df.columns:
        mask = df["question_full"].isin(target_questions)
        df.loc[mask, "category"] = df.loc[mask, "answer"].apply(categorize)

    if "timemodified" in df.columns:
        # first try numeric unix timestamp
        numeric_time = pd.to_numeric(df["timemodified"], errors="coerce")
        dt_from_unix = pd.to_datetime(numeric_time, unit="s", errors="coerce")

        # then try normal datetime text
        dt_from_text = pd.to_datetime(df["timemodified"], errors="coerce")

        # use unix conversion where valid, otherwise use text conversion
        df["timemodified"] = dt_from_unix.fillna(dt_from_text)


    return df


def build_question_chart(answer_dist, group_col, question):
    special_pie_questions = [
        "Data Privacy & Consent: Do you consent to these terms?",
        "Have you used any digital skills to solve a problem or improve a process at your host organization?",
        "How confident are you using digital tools (e.g., email, shared documents, spreadsheets) to complete your work tasks independently?",
        "What device(s) do you primarily use to access digital learning and work tools?",
        "How comfortable are you using AI tools (like ChatGPT, Gemini, Copilot) to carry out work tasks?",
        "Rate your understanding of Data Privacy (e.g., protecting customer information, password security, or GDPR/NDPR basics)."
    ]

    special_bar_questions = [
        "Which statement best describes how the knowledge of technology tools affect your job performance?"
    ]

    if question in special_bar_questions:
        fig = px.bar(
            answer_dist,
            x="count",
            y=group_col,
            orientation="h",
            text="count"
        )
        fig.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=max(400, len(answer_dist) * 60)
        )
        return fig

    if question in special_pie_questions:
        fig = px.pie(
            answer_dist,
            names=group_col,
            values="count",
            hole=0.45
        )
        fig.update_traces(
            textinfo="label+value+percent",
            textposition="outside"
        )
        fig.update_layout(showlegend=False)
        return fig

    num_labels = answer_dist[group_col].nunique()

    if num_labels > 5:
        return None

    fig = px.pie(
        answer_dist,
        names=group_col,
        values="count",
        title=f"Response Distribution: {question}",
        hole=0.45
    )
    fig.update_traces(
        textinfo="label+value+percent",
        textposition="inside"
    )

    return fig


def answer_distribution(question_df, question):
    use_category = "category" in question_df.columns and question_df["category"].notna().any()
    group_col = "category" if use_category else "answer"

    answer_dist = (
        question_df.groupby(group_col)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    if answer_dist.empty:
        return None, None, None

    answer_dist["percentage"] = (
        answer_dist["count"] / answer_dist["count"].sum() * 100
    ).round(2)

    fig = build_question_chart(answer_dist, group_col, question)
    return answer_dist, fig, group_col


# -----------------------------
# UI
# -----------------------------
st.title("Baseline Literacy Assessment Dashboard")

# -----------------------------
# LOAD DATA FROM SAME DIRECTORY
# -----------------------------
# -----------------------------
# LOAD DATA FROM SUPABASE
# -----------------------------

try:
    raw_df = load_data()
    feedback_responses_df = prep_data(raw_df)
    st.success("✅ Data loaded from Supabase database.")

except Exception as e:
    st.error("❌ Failed to load data from Supabase.")
    st.caption(str(e))
    st.stop()

# =========================
# FIX MULTI-SELECT RESPONSES
# =========================

raw_df["answer"] = raw_df["answer"].str.replace("&amp;", "&", regex=False)

options = [
    "Microsoft Office Suite (Word, Excel, PowerPoint)",
    "Google Workspace (Docs, Sheets, Slides)",
    "Email & calendar tools",
    "Design tools",
    "Communication platforms (Slack, Microsoft Teams)",
    "Other:"
]

def extract_categories(text):
    if pd.isna(text):
        return []
    return [opt for opt in options if opt in text]

multi_select_df = raw_df.copy()
multi_select_df["categories"] = multi_select_df["answer"].apply(extract_categories)
multi_select_df = multi_select_df.explode("categories")

# -----------------------------
# SIDEBAR FILTERS
# -----------------------------
st.sidebar.header("Filters")

date_range = None

if "timemodified" in feedback_responses_df.columns:
    min_date = feedback_responses_df["timemodified"].dropna().min()
    max_date = feedback_responses_df["timemodified"].dropna().max()

    if pd.notna(min_date) and pd.notna(max_date):
        date_range = st.sidebar.date_input(
            "Submission Date Range",
            value=(min_date.date(), max_date.date())
        )
    else:
        st.sidebar.info("No valid date data available.")
else:
    st.sidebar.info("Column 'timemodified' not found.")

filtered_df = feedback_responses_df.copy()

if date_range and len(date_range) == 2:
    start_date, end_date = date_range

    if "timemodified" in filtered_df.columns:
        filtered_df = filtered_df[
            filtered_df["timemodified"].dt.date.between(start_date, end_date)
        ]

# -----------------------------
# KPIs
# -----------------------------
total_users = 17516
total_questions = filtered_df["question"].nunique() if "question" in filtered_df.columns else 0

# Define what makes one attempt
attempt_keys = [
    col for col in ["user_id", "course_id", "timemodified"]
    if col in filtered_df.columns
]

if len(attempt_keys) >= 2:
    total_attempts = filtered_df[attempt_keys].drop_duplicates().shape[0]
else:
    total_attempts = 0

total_courses = filtered_df["course_id"].nunique() if "course_id" in filtered_df.columns else 0

total_user = 24000

percentage_of_sudmited_feedback = (total_attempts/total_user)*100

col1, col2, col3, col4 = st.columns(4)
col1.metric("Expected Fellows", total_users)
col2.metric("Total Questions", total_questions)
col3.metric("Total Feedback Submited", total_attempts)
col4.metric("% of Feedback Submited", f"{percentage_of_sudmited_feedback:.2f}%")

# -----------------------------
# TABS
# -----------------------------
tab1, tab2, tab3 = st.tabs([
    "Question Analysis",
    "Time Trend",
    "Multi-select"
])

# -----------------------------
# OV

# -----------------------------
# QUESTION ANALYSIS
# -----------------------------
with tab1:
    st.subheader("Question-Level Breakdown")

    question_order = [
        "Data Privacy & Consent: Do you consent to these terms?",
        "How confident are you using digital tools (e.g., email, shared documents, spreadsheets) to complete your work tasks independently?",
        "Which productivity tools do you currently use regularly in your work? (Select all that apply)",
        'If you picked "Other" above, please specify.',
        "Which of these emerging technologies interests you the most?",
        'If you picked "Other" for the question above, please specify.',
        "Which statement best describes how the knowledge of technology tools affect your job performance?",
        "What digital tasks do you find most difficult at work?",
        "What digital tasks does your current role at your host organization require you to perform?",
        "Which of these would most likely improve your productivity in your current role?",
        "If your answer above is Other, please specify.",
        "What device(s) do you primarily use to access digital learning and work tools?",
        "What is your preferred mode of learning?",
        "Have you used any digital skills to solve a problem or improve a process at your host organization?",
        "If you answered 'Yes' to the previous question, please briefly describe one example.",
        "How comfortable are you using AI tools (like ChatGPT, Gemini, Copilot) to carry out work tasks?",
        "Rate your understanding of Data Privacy (e.g., protecting customer information, password security, or GDPR/NDPR basics)."
    ]

    question_list = (
        filtered_df["question_full"].dropna().unique().tolist()
        if "question_full" in filtered_df.columns
        else []
    )

    question_order_map = {q: i for i, q in enumerate(question_order)}
    question_list = sorted(
        question_list,
        key=lambda x: question_order_map.get(x, 999)
    )

    if not question_list:
        st.warning("No questions available for analysis.")
    else:
        special_pie_questions = [
            "Data Privacy & Consent: Do you consent to these terms?",
            "Have you used any digital skills to solve a problem or improve a process at your host organization?",
            "How confident are you using digital tools (e.g., email, shared documents, spreadsheets) to complete your work tasks independently?",
            "What device(s) do you primarily use to access digital learning and work tools?",
            "How comfortable are you using AI tools (like ChatGPT, Gemini, Copilot) to carry out work tasks?",
            "Rate your understanding of Data Privacy (e.g., protecting customer information, password security, or GDPR/NDPR basics)."
        ]

        special_bar_questions = [
            "Which statement best describes how the knowledge of technology tools affect your job performance?"
        ]

        PRODUCTIVITY_TOOLS_QUESTION = (
                "Which productivity tools do you currently use regularly in your work? (Select all that apply)"
            )
        
        grouped_other_questions = list(OTHER_GROUPING_MAP.keys())
        grouped_if_yes_questions = [IF_YES_QUESTION]

        for question in question_list:
            st.markdown(f"### {question}")

            question_df = filtered_df[
                filtered_df["question_full"] == question
            ].copy()
            

            

            if question == PRODUCTIVITY_TOOLS_QUESTION:
                question_df = multi_select_df[
                    multi_select_df["question_full"] == question
                ].copy()

                question_df = question_df[
                    question_df["categories"].notna() &
                    (question_df["categories"] != "")
                ]

                multi_counts = (
                    question_df["categories"]
                    .value_counts()
                    .reset_index()
                )
                multi_counts.columns = ["category", "count"]

                multi_counts["percentage"] = (
                    multi_counts["count"] / multi_counts["count"].sum() * 100
                ).round(2)

                # Show only table
                st.dataframe(multi_counts, use_container_width=True, hide_index=True)

                st.markdown("---")
                continue
            # Grouped handling for "Other, specify" questions
            if question in grouped_other_questions:
                grouped_counts = get_grouped_question_counts(question_df, question)

                if grouped_counts.empty:
                    st.info("No responses available for this question.")
                else:
                    fig = px.bar(
                        grouped_counts,
                        x="count",
                        y="category",
                        orientation="h",
                        text="count",
                    )
                    fig.update_layout(
                        yaxis={"categoryorder": "total ascending"},
                        height=max(400, len(grouped_counts) * 55)
                    )

                    st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")
                continue  # 🔥 VERY IMPORTANT

            # Grouped handling for "If yes..." question
            if question in grouped_if_yes_questions:
                grouped_counts = get_if_yes_grouped_counts(question_df)

                if grouped_counts.empty:
                    st.info("No responses available for this question.")
                else:
                    fig = px.bar(
                        grouped_counts,
                        x="count",
                        y="category",
                        orientation="h",
                        text="count",
                    )
                    fig.update_layout(
                        yaxis={"categoryorder": "total ascending"},
                        height=max(420, len(grouped_counts) * 55)
                    )

                    # Show only chart
                    st.plotly_chart(fig, use_container_width=True)

                st.markdown("---")
                continue  # 🔥 important

            answer_dist, fig, group_col = answer_distribution(question_df, question)

            if answer_dist is None:
                st.info("No responses available for this question.")
            else:
                num_labels = answer_dist[group_col].nunique()

                if question in special_pie_questions:
                    st.plotly_chart(fig, use_container_width=True)

                elif question in special_bar_questions:
                    st.plotly_chart(fig, use_container_width=True)

                elif num_labels > 5:
                    st.dataframe(answer_dist, use_container_width=True, hide_index=True)

                else:
                    left, right = st.columns([1.3, 1])
                    with left:
                        st.plotly_chart(fig, use_container_width=True)
                    with right:
                        st.dataframe(answer_dist, use_container_width=True, hide_index=True)

            st.markdown("---")

# -----------------------------
# TIME TREND
# -----------------------------
with tab2:
    st.subheader("Attempts Over Time")

    if "timemodified" in filtered_df.columns:
        time_df = filtered_df.copy()

        time_df = time_df.dropna(subset=["timemodified"])

        if not time_df.empty:
            time_df["date"] = time_df["timemodified"].dt.date

            attempt_keys = [
                col for col in ["fullname", "course_name", "timemodified"]
                if col in time_df.columns
            ]

            if len(attempt_keys) >= 2:
                attempts_df = time_df[attempt_keys + ["date"]].drop_duplicates()

                daily_attempts = (
                    attempts_df.groupby("date")
                    .size()
                    .reset_index(name="attempts")
                    .sort_values("date")
                )

                fig_time = px.line(
                    daily_attempts,
                    x="date",
                    y="attempts",
                    title="Attempts Over Time",
                    markers=True
                )

                st.plotly_chart(fig_time, use_container_width=True)
            else:
                st.info("Not enough fields available to calculate attempts uniquely.")
        else:
            st.info("No valid time data available.")
    else:
        st.info("Column 'timemodified' not found in the dataset.")

# -----------------------------
# MULTI-SELECT
# -----------------------------
with tab3:
    st.subheader("Multi-select Question Analysis")

    if "answer" not in filtered_df.columns or "question_full" not in filtered_df.columns:
        st.info("Required columns for multi-select analysis are missing.")
    else:
        multi_check = filtered_df.copy()
        multi_check["is_multi"] = multi_check["answer"].astype(str).str.contains("\n", na=False)

        multi_questions = (
            multi_check.groupby("question_full")["is_multi"]
            .sum()
            .reset_index(name="multi_response_count")
        )
        multi_questions = multi_questions[multi_questions["multi_response_count"] > 0]

        if multi_questions.empty:
            st.info("No multi-select questions found.")
        else:
            selected_multi_question = st.selectbox(
                "Select a multi-select question",
                multi_questions["question_full"].tolist()
            )

            df_multi = filtered_df[filtered_df["question_full"] == selected_multi_question].copy()
            df_multi["answer_split"] = df_multi["answer"].astype(str).str.split("\n")
            df_multi = df_multi.explode("answer_split")
            df_multi["answer_split"] = df_multi["answer_split"].astype(str).str.strip()
            df_multi = df_multi[df_multi["answer_split"] != ""]

            multi_counts = (
                df_multi.groupby("answer_split")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )

            if not multi_counts.empty:
                multi_counts["percentage"] = (
                    multi_counts["count"] / multi_counts["count"].sum() * 100
                ).round(2)

                fig_multi = px.bar(
                    multi_counts,
                    x="answer_split",
                    y="count",
                    title=f"Multi-select Breakdown: {selected_multi_question}"
                )

                st.plotly_chart(fig_multi, use_container_width=True)
                st.dataframe(multi_counts, use_container_width=True, hide_index=True)
            else:
                st.info("No multi-select responses found for this question.")