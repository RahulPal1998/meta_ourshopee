from os import name
"""
Meta Marketing API to BigQuery ETL Pipeline
Fetches data from Meta Marketing API and loads into BigQuery tables, 
auto-creating the tables if they don't exist, and includes an audit log.
"""

import json
import sys
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery import SchemaField, Table

# Ensure the Facebook library is present
try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
except ImportError:
    print("The 'facebook-business' library is not installed.")
    print("Please run: pip install facebook-business")
    sys.exit()

# --- Configuration ---
META_CONFIG = {
    'app_id': '1156038156454409',
    'app_secret': '3f144617fbd9cffec3254e63110000c1',
    'access_token': 'EAAQbaRupigkBPiPXr8ouA9fBOaqlqGiFWthtYYQkTaCRYNgtjRXhCxgFMR6HjTnKWFZAagNLd3c7fWUVFC3uGIs0SJgQcCGgttX75QpFlftekbcr5di4v3sHNM32ptlrFyPHpv9AoHuZAzov0T6BdNBTcVkAsdvy0MFfxcM4uO2J5EMwM4zk9jH8fFDeo2Jdqq7Gm4c0p2qEHWtWMI79zPqZAyCeOUtvAZACwmAPt4sGJgZDZD',
    'ad_account_id': 'act_1401816280975925'
}

# BQ_CONFIG = {
#     'project_id': 'ourshopee-459907',
#     'dataset_id': 'meta_ads_data',
#     # 'service_account_key_path': '/content/ourshopee.json'
# }

# --- Configuration ---
# ... (META_CONFIG remains the same)

BQ_CONFIG = {
    'project_id': 'ourshopee-459907',
    'dataset_id': 'meta_ads_data',
    # 'service_account_key_path': '/content/ourshopee.json'  <-- REMOVE THIS LINE
}

# --- Initialize Clients ---
def initialize_bigquery_client():
    """Initializes and returns the BigQuery client using Application Default Credentials (ADC)."""
    # Initialize without credentials file; it will automatically pick up the 
    # service account assigned to the Cloud Run Job.
    client = bigquery.Client(
        project=BQ_CONFIG['project_id']
    )
    print("✓ BigQuery client initialized using ADC")
    return client

# # --- Initialize Clients ---
# def initialize_bigquery_client():
#     """Initializes and returns the BigQuery client."""
#     credentials = service_account.Credentials.from_service_account_file(
#         BQ_CONFIG['service_account_key_path']
#     )
#     client = bigquery.Client(
#         credentials=credentials,
#         project=BQ_CONFIG['project_id']
#     )
#     print("✓ BigQuery client initialized")
#     return client

def initialize_meta_api():
    """Initializes the Meta Ads API."""
    FacebookAdsApi.init(
        META_CONFIG['app_id'],
        META_CONFIG['app_secret'],
        META_CONFIG['access_token']
    )
    print("✓ Meta API initialized")

# --- NEW: Audit Log Table Creation ---
def ensure_audit_log_table(client):
    """Checks if the audit log table exists and creates it if it doesn't."""
    audit_table_name = 'etl_audit_log'
    full_audit_table_id = f"{BQ_CONFIG['project_id']}.{BQ_CONFIG['dataset_id']}.{audit_table_name}"
    
    schema = [
        SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("table_name", "STRING", mode="REQUIRED"),
        SchemaField("rows_processed", "INTEGER", mode="NULLABLE"),
        SchemaField("status", "STRING", mode="REQUIRED"),
        SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]
    
    table = Table(full_audit_table_id, schema=schema)
    
    try:
        # Check if the table exists
        client.get_table(table)
        print(f"✓ Audit Log Table '{audit_table_name}' already exists.")
    except bigquery.exceptions.NotFound:
        # If it does not exist, create it
        client.create_table(table)
        print(f"✨ Created Audit Log Table: {audit_table_name}")
    except Exception as e:
        print(f"🔴 ERROR during audit table check/creation: {e}")
        raise # Re-raise to stop the pipeline if the audit log cannot be secured

# --- Fetch Meta Data ---
def fetch_meta_data(last_run_time_insight):
    """Fetches campaign, adset, ad, and insight data from the Meta API."""
    ad_account = AdAccount(META_CONFIG['ad_account_id'])
    data = {}

    # Campaigns
    campaigns = ad_account.get_campaigns(
        fields=['id', 'name', 'objective', 'status', 'start_time', 'stop_time']
    )
    data['campaigns'] = [c.export_all_data() for c in campaigns]

    # Adsets
    adsets = ad_account.get_ad_sets(
        fields=['id', 'name', 'campaign_id', 'status', 'targeting']
    )
    data['adsets'] = [a.export_all_data() for a in adsets]

    # Ads
    ads = ad_account.get_ads(
        fields=['id', 'name', 'adset_id', 'campaign_id', 'status', 'creative']
    )
    data['ads'] = [a.export_all_data() for a in ads]

    # Insights - only last day for incremental
    end_date = datetime.now()
    start_date = last_run_time_insight
    insights = ad_account.get_insights(
        fields=[
            'date_start', 'impressions', 'clicks', 'spend', 
            'cpm', 'cpc', 'reach', 'campaign_id', 'adset_id', 'ad_id'
        ],
        params={
            'time_range': {
                'since': start_date.strftime('%Y-%m-%d'),
                'until': end_date.strftime('%Y-%m-%d')
            },
            'level': 'ad',
            'time_increment': 1
        }
    )
    data['insights'] = [i.export_all_data() for i in insights]

    print(f"Fetched: {len(data['campaigns'])} campaigns, {len(data['adsets'])} adsets, {len(data['ads'])} ads, {len(data['insights'])} insights")
    return data

# --- BigQuery Load Function for Data Tables ---
def load_data_to_bigquery(client, df, table_name):
    """
    Loads a pandas DataFrame into BigQuery, inferring schema and handling 
    WRITE_APPEND/WRITE_TRUNCATE based on table type.
    """
    full_table_id = f"{client.project}.{BQ_CONFIG['dataset_id']}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=(
            bigquery.WriteDisposition.WRITE_APPEND if table_name == 'insights' 
            else bigquery.WriteDisposition.WRITE_TRUNCATE
        ),
        time_partitioning=bigquery.TimePartitioning(
            field="last_run_timestamp",
            type_=bigquery.TimePartitioningType.DAY
        ) if table_name == 'insights' else None
    )

    print(f"🚀 Starting load job for table: {full_table_id}")
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    print(f"🎉 Successfully loaded {len(df)} rows to BigQuery table: {full_table_id}")


# --- Audit Log Function ---
def log_audit_entry(client, run_timestamp, table_name, rows_processed, status, error_message=None):
    """Loads a single log entry into the dedicated ETL Audit Log table."""
    audit_table_name = 'etl_audit_log'
    full_audit_table_id = f"{client.project}.{BQ_CONFIG['dataset_id']}.{audit_table_name}"

    log_data = [{
        'run_timestamp': run_timestamp.isoformat(),
        'table_name': table_name,
        'rows_processed': rows_processed,
        'status': status,
        'error_message': str(error_message) if error_message else None 
    }]
    
    log_df = pd.DataFrame(log_data)
    log_df['run_timestamp'] = pd.to_datetime(log_df['run_timestamp'])

    # Always APPEND to the log table
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    
    try:
        job = client.load_table_from_dataframe(log_df, full_audit_table_id, job_config=job_config)
        job.result()
        print(f"✅ Logged audit entry for {table_name}.")
    except Exception as e:
        # Use sys.stderr for critical, non-logged errors
        sys.stderr.write(f"🔴 FATAL ERROR: Failed to log audit entry for {table_name}. Error: {e}\n")

def last_run_time_for_insight(client):
  table_name = 'etl_audit_log'
  full_table_id = f"{client.project}.{BQ_CONFIG['dataset_id']}.{table_name}"
  query = f"""
    SELECT MAX(run_timestamp) AS last_run_timestamp
    FROM `{full_table_id}`where table_name = 'insights'
  """
  query_job = client.query(query)
  results = query_job.result()
  for row in results:
    return row.last_run_timestamp


# --- Main Execution ---
def main():
    bq_client = initialize_bigquery_client()
    initialize_meta_api()
    
    # 1. Ensure the Audit Log Table Exists FIRST
    ensure_audit_log_table(bq_client)

    # 2. Prepare Timestamps
    run_timestamp_dt = datetime.now()
    run_timestamp_str = run_timestamp_dt.isoformat() 
    last_run_time_insight = last_run_time_for_insight(bq_client)

    # 3. Fetch Data
    try:
        meta_data = fetch_meta_data(last_run_time_insight)
        meta_data_py = json.loads(json.dumps(meta_data))
    except Exception as e:
        # Log a high-level failure if fetching fails entirely
        log_audit_entry(bq_client, run_timestamp_dt, "FETCH_ALL", 0, "FATAL_FAILURE", str(e))
        print(f"🔴 Fatal Error during data fetch: {e}")
        return

    # 4. Create and Prepare DataFrames
    campaigns = pd.DataFrame(meta_data_py.get('campaigns', []))
    adsets = pd.DataFrame(meta_data_py.get('adsets', []))
    ads = pd.DataFrame(meta_data_py.get('ads', []))
    insights = pd.DataFrame(meta_data_py.get('insights', []))

    # Add and convert the audit timestamp column (CRITICAL FIX)
    for df in [campaigns, adsets, ads, insights]:
        df['last_run_timestamp'] = run_timestamp_dt
    
    # Convert 'date_start' in insights for proper BQ date/time type
    if not insights.empty and 'date_start' in insights.columns:
        insights['date_start'] = pd.to_datetime(insights['date_start'])
        
    dataframes_to_load = {
        'campaigns': campaigns,
        'adsets': adsets,
        'ads': ads,
        'insights': insights
    }



    # 5. Load DataFrames to BigQuery with Logging
    for table_name, df in dataframes_to_load.items():
        rows_processed = len(df)
        
        if rows_processed == 0:
            print(f"Skipping {table_name}: DataFrame is empty.")
            log_audit_entry(bq_client, run_timestamp_dt, table_name, 0, "SKIPPED")
            continue
            
        try:
            load_data_to_bigquery(bq_client, df, table_name)
            
            # 🟢 Log SUCCESS
            log_audit_entry(bq_client, run_timestamp_dt, table_name, rows_processed, "SUCCESS")

        except Exception as e:
            print(f"❌ An error occurred while loading {table_name} to BigQuery: {e}")
            
            # 🔴 Log FAILURE
            log_audit_entry(bq_client, run_timestamp_dt, table_name, rows_processed, "FAILURE", str(e))

if __name__ == "__main__":
    main()
