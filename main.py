import os
from flask import Flask
import pandas as pd
from google.cloud import bigquery
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from datetime import datetime, timedelta, timezone

# --------------------------
# CONFIG
ACCESS_TOKEN = "EAAQbaRupigkBPsfUmnOmc1VSy4mkMwCBFiPcgYvSZCSFooqxC4TuZCHO1mjT6LXg6k2E30BXPQYwOZBL9mebEv4t4wRgDsLj5mDDhkfVCJrwG9LQGapSOGAMUEtBVVUG7omdzfvMeBTzvCMb0S0w3qTcYpRdzN82lx7ZB79qXG5VDbOm2bxQRza5N6rjPUbjiIpZB9I3Hj7s3PDz3TO3RzRq1B3iQv7HfnWgdXPpe6rZCsRAZDZD"
ACCOUNT_ID = "act_1815733095432159"   # replace with your ad account id
# ACCESS_TOKEN = "YOUR_FACEBOOK_ACCESS_TOKEN"
# ACCOUNT_ID = "act_XXXXXXXXXXXX"
PROJECT_ID = "ourshopee-459907"
DATASET = "meta_ads_data_set"

FINAL_TABLE = f"{PROJECT_ID}.{DATASET}.fb_campaigns"
AUDIT_TABLE = f"{PROJECT_ID}.{DATASET}.audit_lst_tm_stmp"
PROCESS_NAME = "fb_campaigns"

# Initialize Flask app
app = Flask(__name__)

# Init Facebook API
FacebookAdsApi.init(access_token=ACCESS_TOKEN)
ad_account = AdAccount(ACCOUNT_ID)

# BigQuery client uses default credentials
bq = bigquery.Client(project=PROJECT_ID)

# --------------------------
def get_last_run():
    query = f"""
      SELECT last_update_time
      FROM `{AUDIT_TABLE}`
      WHERE process_name = '{PROCESS_NAME}'
      ORDER BY last_update_time DESC
      LIMIT 1
    """
    rows = list(bq.query(query).result())
    if rows:
        return rows[0].last_update_time
    else:
        return datetime(2000, 1, 1, tzinfo=timezone.utc)

def update_audit_table(new_time: datetime, job_run_time: datetime):
    rows = [{
        "process_name": PROCESS_NAME,
        "last_update_time": new_time.isoformat(),
        "last_job_run": job_run_time.isoformat()
    }]
    errors = bq.insert_rows_json(AUDIT_TABLE, rows)
    if errors:
        raise RuntimeError(f"Audit update failed: {errors}")

def fetch_campaigns(since_time: datetime, job_run_time: datetime):
    fields = [
        "id", "account_id", "name", "status", "objective",
        "start_time", "stop_time", "daily_budget", "lifetime_budget",
        "spend_cap", "bid_strategy", "special_ad_categories", "updated_time"
    ]
    since_time_safe = since_time - timedelta(minutes=5)
    params = {
        "filtering": [{
            "field": "updated_time",
            "operator": "GREATER_THAN",
            "value": since_time_safe.isoformat()
        }]
    }
    cursor = ad_account.get_campaigns(fields=fields, params=params)
    all_data = [obj.export_all_data() for obj in cursor]
    df = pd.DataFrame(all_data)
    df["last_job_run"] = job_run_time
    return df

def load_to_bigquery(df):
    if df.empty:
        return 0
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_dataframe(df, FINAL_TABLE, job_config=job_config).result()
    return len(df)

def run_etl():
    job_run_time = datetime.now(timezone.utc)
    last_run = get_last_run()
    df = fetch_campaigns(last_run - timedelta(minutes=5), job_run_time)
    count = load_to_bigquery(df)
    if count > 0:
        new_max_time = pd.to_datetime(df["updated_time"]).max().to_pydatetime()
        update_audit_table(new_max_time, job_run_time)
    return f"ETL completed. {count} campaigns processed."

# --------------------------
@app.route("/", methods=["GET"])
def index():
    try:
        result = run_etl()
        return result, 200
    except Exception as e:
        return f"ETL failed: {str(e)}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
