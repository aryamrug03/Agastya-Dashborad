# src/aggregator.py
import os
import glob
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# --- Configuration (edit as needed) ---
DATA_DIR = "data"
OUT_DIR = "docs"
EXCLUDED_PROGRAMS = [
    "Digikshetra", "Financial Literacy", "I-code", "IMSL-MATH",
    "Young Instructor Training", "Jignyasa", "Ecology", "Library",
    "Art & Craft", "Science Model Making", "Debate/Quiz",
    "Campus Tour", "None", "Plastic Waste Management", "Circle Time",
    "YAP Program", "IA Pre/Post", "Team Building Activity"
]
# Column mapping -- change if your files use different names
COLUMN_MAP = {
    'region': ['Region','region'],
    'ignator_id': ['IgnatorID','Ignator Id','Q-Card ID','Ignator'],
    'student_id': ['StudentID','Student Id','Student'],
    'session_id': ['SessionID','Session Id','Session'],
    'program': ['Program','Program Name'],
    'session_date': ['SessionDate','Date'],
    'da_pre_score': ['DA_Pre_Score','DA Pre Score','DA Pre'],
    'da_post_score': ['DA_Post_Score','DA Post Score','DA Post'],
    'da_pre_date': ['DA_Pre_Date','DA Pre Date'],
    'da_post_date': ['DA_Post_Date','DA Post Date'],
}

# --- Helpers ---
def find_col(df, options):
    for o in options:
        if o in df.columns:
            return o
    return None

def normalize_df(df):
    out = pd.DataFrame()
    for key, opts in COLUMN_MAP.items():
        col = find_col(df, opts)
        out[key] = df[col] if col is not None else np.nan
    return out

# --- Read all excels ---
def load_all_data():
    files = glob.glob(os.path.join(DATA_DIR,"*.xls*"))
    frames = []
    for f in files:
        try:
            xls = pd.read_excel(f, sheet_name=None)
            for sheet_name, sheet in xls.items():
                df = normalize_df(sheet)
                # region fallback to filename if missing
                if df['region'].isnull().all():
                    basename = os.path.basename(f)
                    df['region'] = os.path.splitext(basename)[0]
                frames.append(df)
        except Exception as e:
            print("Error reading", f, e)
    if not frames:
        return pd.DataFrame(columns=list(COLUMN_MAP.keys()))
    return pd.concat(frames, ignore_index=True)

def apply_business_rules(df):
    # cast dates
    df['session_date'] = pd.to_datetime(df['session_date'], errors='coerce')
    df['da_pre_date'] = pd.to_datetime(df['da_pre_date'], errors='coerce')
    df['da_post_date'] = pd.to_datetime(df['da_post_date'], errors='coerce')
    # filter out excluded programs
    df['program'] = df['program'].astype(str)
    df['is_excluded_program'] = df['program'].isin(EXCLUDED_PROGRAMS)
    # Condition for counting a DA as valid pre/post pair:
    # both pre and post present AND dates are same (PDF excludes mismatched).
    df['has_pre'] = df['da_pre_score'].notna()
    df['has_post'] = df['da_post_score'].notna()
    df['prepost_same_date'] = (df['da_pre_date'].notna()) & (df['da_post_date'].notna()) & (df['da_pre_date'].dt.date == df['da_post_date'].dt.date)
    df['valid_prepost_pair'] = df['has_pre'] & df['has_post'] & df['prepost_same_date']
    return df

def compute_metrics(df):
    out_rows = []
    regions = df['region'].fillna('Unknown').unique()
    for r in sorted(regions):
        sub = df[df['region']==r]
        # Session Count after exclusion : unique sessions where program not excluded
        eligible_sessions = sub[~sub['is_excluded_program']]
        session_count = eligible_sessions['session_id'].nunique()
        unique_students = sub['student_id'].nunique()
        unique_ignators = sub['ignator_id'].nunique()
        da_pre_count = sub['has_pre'].sum()
        da_post_count = sub['has_post'].sum()
        # enforce same-date rule for score comparisons if desired
        gap = int(da_pre_count - da_post_count)
        wastage_pct = (gap / da_pre_count * 100) if da_pre_count>0 else 0.0
        completion_rate = (da_post_count / session_count * 100) if session_count>0 else 0.0
        # ignators who had eligible sessions but no DA (both pre and post missing)
        ignators_with_sessions = eligible_sessions['ignator_id'].dropna().unique()
        ignator_did_no_da = []
        for ign in ignators_with_sessions:
            ign_rows = eligible_sessions[eligible_sessions['ignator_id']==ign]
            # if there are rows and all have no pre and no post:
            if ign_rows['has_pre'].sum() == 0 and ign_rows['has_post'].sum() == 0:
                ignator_did_no_da.append(ign)
        out_rows.append({
            'Region': r,
            'Unique Students': int(unique_students if pd.notna(unique_students) else 0),
            'Unique Ignators': int(unique_ignators if pd.notna(unique_ignators) else 0),
            'Session Count (After Exclusion)': int(session_count),
            'DA-Pre': int(da_pre_count),
            'DA-Post': int(da_post_count),
            'Completion Rate (%)': round(completion_rate,1),
            'DA Pre-to-Post Gap': gap,
            '% Incomplete DA-Set (Wastage)': round(wastage_pct,1),
            'Ignators w/eligible sessions but no DA (count)': len(ignator_did_no_da),
            'Ignators missing DA (list)': ignator_did_no_da
        })
    # TOTAL row
    agg = pd.DataFrame(out_rows)
    total_row = {
        'Region':'TOTAL',
        'Unique Students': int(df['student_id'].nunique()),
        'Unique Ignators': int(df['ignator_id'].nunique()),
        'Session Count (After Exclusion)': int(df[~df['is_excluded_program']]['session_id'].nunique()),
        'DA-Pre': int(df['has_pre'].sum()),
        'DA-Post': int(df['has_post'].sum()),
        'Completion Rate (%)': round((df['has_post'].sum() / max(1, df[~df['is_excluded_program']]['session_id'].nunique()))*100,1),
        'DA Pre-to-Post Gap': int(df['has_pre'].sum() - df['has_post'].sum()),
        '% Incomplete DA-Set (Wastage)': round(((df['has_pre'].sum() - df['has_post'].sum())/max(1, df['has_pre'].sum()))*100,1),
        'Ignators w/eligible sessions but no DA (count)': 0,
        'Ignators missing DA (list)': []
    }
    agg = pd.concat([agg, pd.DataFrame([total_row])], ignore_index=True)
    return agg

def render_dashboard(agg_df, out_dir=OUT_DIR):
    os.makedirs(out_dir, exist_ok=True)
    agg_df.to_csv(os.path.join(out_dir,'aggregated_metrics.csv'), index=False)
    # Example: bar chart DA-Pre vs DA-Post
    df = agg_df[agg_df['Region']!='TOTAL']
    fig = go.Figure()
    fig.add_trace(go.Bar(name='DA-Pre', x=df['Region'], y=df['DA-Pre']))
    fig.add_trace(go.Bar(name='DA-Post', x=df['Region'], y=df['DA-Post']))
    fig.update_layout(barmode='group', title='DA Pre vs Post by Region',
                      xaxis_title='Region', yaxis_title='Count')
    html_out = os.path.join(out_dir, 'index.html')
    fig.write_html(html_out, include_plotlyjs='cdn', full_html=True)
    print("Wrote", html_out, "and aggregated CSV")

def main():
    print("Loading data...")
    df = load_all_data()
    if df.empty:
        print("No data found in", DATA_DIR)
        return
    df = apply_business_rules(df)
    agg = compute_metrics(df)
    render_dashboard(agg)

if __name__ == "__main__":
    main()

