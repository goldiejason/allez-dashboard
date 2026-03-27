# Allez Fencing Dashboard 🤺

Fencing performance analytics dashboard for Allez Fencing — powered by FencingTimeLive and UK Ratings data.

## Architecture

| Layer | Tech | Purpose |
|-------|------|---------|
| Database | Supabase (PostgreSQL) | Persistent storage of all bout data |
| Collection | Python scripts | Pull data from FTL + UK Ratings |
| Analytics | `metrics/calculator.py` | Compute all statistics from real bout data |
| Dashboard | Streamlit | Display and refresh UI |
| Automation | GitHub Actions | Weekend data refresh |

## Data Sources

- **FencingTimeLive (FTL)**: Pool bouts (individual scores, opponents, touches), DE bouts, event dates, international events
- **UK Ratings**: Annual pool W/L totals only

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your Supabase keys
```

### 3. Run the Supabase schema

Copy the contents of `database/schema.sql` and run it in the [Supabase SQL Editor](https://supabase.com/dashboard/project/whffigkbnkugzsltkdhq/sql).

### 4. Add athletes to the database

Use the Supabase Table Editor to add athlete records. Required fields:
- `name_display`: Full name
- `name_ftl`: Exact name as shown on FTL
- `ftl_fencer_id`: FTL internal ID (find from their FTL profile URL)
- `uk_ratings_id`: UK Ratings ID (from their profile URL)
- `weapon`: foil / epee / sabre

### 5. Run a data collection

```bash
python scripts/run_weekly_refresh.py
```

### 6. Launch the dashboard

```bash
streamlit run app.py
```

## GitHub Actions — Weekend Automation

The workflow in `.github/workflows/weekend_refresh.yml` runs every Saturday at 23:00 UTC.

To set it up, add these secrets in GitHub → Settings → Secrets → Actions:
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`

## Deployment

Deploy to [Streamlit Community Cloud](https://streamlit.io/cloud):
1. Connect the GitHub repo
2. Set secrets in the Streamlit dashboard (same keys as above)
3. Main file: `app.py`
