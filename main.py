import pandas as pd
from google.cloud import bigquery
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

# Replace with your uploaded JSON filename
SERVICE_ACCOUNT_FILE = "ourshopee.json"

# Authenticate
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
bq_client = bigquery.Client(credentials=creds, project=creds.project_id)

# --------------------------
# CONFIG
ACCESS_TOKEN = "EAAQbaRupigkBPsfUmnOmc1VSy4mkMwCBFiPcgYvSZCSFooqxC4TuZCHO1mjT6LXg6k2E30BXPQYwOZBL9mebEv4t4wRgDsLj5mDDhkfVCJrwG9LQGapSOGAMUEtBVVUG7omdzfvMeBTzvCMb0S0w3qTcYpRdzN82lx7ZB79qXG5VDbOm2bxQRza5N6rjPUbjiIpZB9I3Hj7s3PDz3TO3RzRq1B3iQv7HfnWgdXPpe6rZCsRAZDZD"
ACCOUNT_ID = "act_1815733095432159"   # replace with your ad account id
app_id = '1156038156454409'
app_secret = '3f1446169fbd9cffec3254e63110000c1'
PROJECT_ID = "ourshopee-459907"
DATASET = "meta_ads_data_set"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET}.fb_campaigns"
AUDIT_TABLE = f"{PROJECT_ID}.{DATASET}.audit_lst_tm_stmp"
PROCESS_NAME = "fb_campaigns"

# Init Facebook API
FacebookAdsApi.init(access_token=ACCESS_TOKEN)
ad_account = AdAccount(ACCOUNT_ID)

# BigQuery client
bq = bigquery.Client(credentials=creds, project=creds.project_id)

# --------------------------
def get_last_run():
    """Fetch last update timestamp from audit table"""
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
        return datetime(2000, 1, 1, tzinfo=timezone.utc)  # default if first run

def update_audit_table(new_time: datetime, job_run_time: datetime):
    """Update audit table with latest update_time and job_run_time"""
    # Convert datetime objects to ISO 8601 strings for JSON serialization
    rows = [{
        "process_name": PROCESS_NAME,
        "last_update_time": new_time.isoformat(),
        "last_job_run": job_run_time.isoformat()
    }]
    errors = bq.insert_rows_json(AUDIT_TABLE, rows)
    if errors:
        raise RuntimeError(f"Audit update failed: {errors}")

def fetch_campaigns(since_time: datetime, job_run_time: datetime):
    """Fetch campaigns updated since last run"""
    fields = [
        "id", "account_id", "name", "status", "objective",
        "start_time", "stop_time", "daily_budget", "lifetime_budget",
        "spend_cap", "bid_strategy", "special_ad_categories", "updated_time"
    ]

    # Add safety buffer of 5 min to avoid misses records
    since_time_safe = since_time - timedelta(minutes=5)

    params = {
        "filtering": [{
            "field": "updated_time",
            "operator": "GREATER_THAN",
            "value": since_time_safe.isoformat()
        }]
    }

    cursor = ad_account.get_campaigns(fields=fields, params=params)
    all_data = []
    for obj in cursor:
        all_data.append(obj.export_all_data())

    df = pd.DataFrame(all_data)

    # add last_job_run column
    df["last_job_run"] = job_run_time
    # df["updated_time"] = pd.to_datetime(df["updated_time"])

    return df

# def load_to_bigquery(df):
def load_to_bigquery(df):
    """Full refresh: overwrite final table"""
    if df.empty:
        print("⚠️ No data to load")
        return

    table_id = FINAL_TABLE  # use the constant you defined
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    bq.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    print(f"✅ {len(df)} rows written to {table_id}")
    # """Merge campaigns into final table"""
    # job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    # staging_table = f"{PROJECT_ID}.{DATASET}._stg_fb_campaigns"

    # # 1. Load into staging
    # bq.load_table_from_dataframe(df, staging_table, job_config=job_config).result()

    # # 2. Merge into final
    # merge_sql = f"""
    #   MERGE `{FINAL_TABLE}` T
    #   USING `{staging_table}` S
    #   ON T.id = S.id
    #   WHEN MATCHED THEN
    #     UPDATE SET *
    #   WHEN NOT MATCHED THEN
    #     INSERT ROW
    # """
    # bq.query(merge_sql).result()

    # # 3. Clear staging
    # bq.query(f"TRUNCATE TABLE `{staging_table}`").result()

# --------------------------
# MAIN FLOW
last_run = get_last_run()
job_run_time = datetime.now(timezone.utc)
print(f"job run: {job_run_time.isoformat()}")

# add 5-min safety buffer to avoid missed records
last_run_safe = last_run - timedelta(minutes=5)

print(f"Fetching campaigns updated since {last_run_safe}...")

df = fetch_campaigns(last_run_safe,job_run_time)

if not df.empty:
    print(f"✅ Fetched {len(df)} campaigns")
    load_to_bigquery(df)

    new_max_time = pd.to_datetime(df["updated_time"]).max().to_pydatetime()
    update_audit_table(new_max_time,job_run_time)

    print(f"✅ Updated audit table with {new_max_time}")
else:
    print("⚠️ No new campaigns found")