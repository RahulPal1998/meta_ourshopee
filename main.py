"""
Meta Marketing API to BigQuery ETL Pipeline
Fetches Facebook and Instagram ad data, logging audit entries using pandas-gbq

FIXED:
1. CRITICAL: Implements PAGING for fetching Campaigns, Adsets, and Ads (structure data) 
   with a 2-second sleep between each page request to avoid "User request limit reached" 
   for high-volume accounts.
2. Insight fetching logic uses date strings (confirmed stable).
3. Stricter global rate limit handling (30s rest, 180s backoff) is maintained.
"""

import json
import sys
from datetime import datetime, timedelta
import pandas as pd
import time 
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, Table
import pandas_gbq 

# Ensure the Facebook library is present
try:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
except ImportError:
    print("The 'facebook-business' library is not installed.")
    sys.exit()

# --- Configuration (Kept the same) ---
META_CONFIG = {
    'app_id': '1156038156454409',
    'app_secret': '3f144617fbd9cffec3254e63110000c1',
    'access_token': 'EAAQbaRupigkBPqd7Lyvw2kWhHG0znI2WFwZC7Km5ZCNcwWJMvICMGKKQ0QVt2GqS0dSsoePRHnxC1k5ybmlkHXgROWMCHMcAGaizmsZCZBJNFckSaZBzyl4nufnmIi2HcaPYVL85PqxBRYNZA4Y6EpIdxmKmx0RX3HzewhjZBJDFy12rYfw6qZCthFY5FxsskIZBuNgZDZD',
    'ad_account_ids': [
        'act_1401816280975925',
        'act_1815733095432159',
        'act_3145430672387483',
        'act_254058322308777',
        'act_1312101778985255',
        'act_555810741726052',
        'act_209003610403715'
    ]
}

BQ_CONFIG = {
    'project_id': 'ourshopee-459907',
    'dataset_id': 'meta_ads_data',
#    'service_account_path': 'ourshopee.json', 
}

# --- Initialize Clients (Kept the same) ---

def initialize_bigquery_client():
    """Initializes the BigQuery client using Application Default Credentials (ADC)."""
    
    # Initialize without arguments. The client library automatically uses
    # the service account assigned to the Cloud Run Job (ADC).
    client = bigquery.Client(
        project=BQ_CONFIG['project_id']
    )
    print("✓ BigQuery client initialized using Application Default Credentials")
    return client

def initialize_meta_api():
    """Initializes the Facebook Marketing API."""
    FacebookAdsApi.init(
        META_CONFIG['app_id'],
        META_CONFIG['app_secret'],
        META_CONFIG['access_token']
    )
    print("✓ Meta API initialized")

# --- Audit and Table Creation (Kept the same) ---
def create_bigquery_dataset(client):
    """Checks if the dataset exists and creates it if it doesn't."""
    dataset_id = BQ_CONFIG['dataset_id']
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref) 
        print(f"✓ Dataset '{dataset_id}' exists.")
    except Exception as e:
        if 'Not found' in str(e) or '404' in str(e):
            client.create_dataset(bigquery.Dataset(dataset_ref)) 
            print(f"✨ Created Dataset: {dataset_id}")
        else:
            raise e

def ensure_audit_log_table(client):
    """Ensures the audit log table exists with the correct (REQUIRED) schema."""
    audit_table_name = 'etl_audit_log'
    full_audit_table_id = f"{BQ_CONFIG['project_id']}.{BQ_CONFIG['dataset_id']}.{audit_table_name}"
    
    schema = [
        SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
        SchemaField("table_name", "STRING", mode="REQUIRED"),
        SchemaField("ad_account_id", "STRING", mode="REQUIRED"), 
        SchemaField("rows_processed", "INTEGER", mode="NULLABLE"),
        SchemaField("status", "STRING", mode="REQUIRED"),
        SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]
    
    table = Table(full_audit_table_id, schema=schema)
    
    try:
        client.get_table(table)
        print(f"✓ Audit Log Table '{audit_table_name}' exists (schema is assumed correct).")
        
    except Exception as e:
        if 'Not found' in str(e) or '404' in str(e):
            client.create_table(table)
            print(f"✨ Created Audit Log Table: {audit_table_name} with REQUIRED ad_account_id.")
        else:
            print(f"🔴 ERROR during audit table check: {e}")
            raise 

# --- New Helper Function for Paging ---
def fetch_paged_data_safely(fetch_method, fields, entity_name):
    """
    Fetches data using pagination, sleeping between each page request to respect limits.
    
    Args:
        fetch_method (callable): e.g., ad_account.get_campaigns
        fields (list): Fields to request.
        entity_name (str): 'Campaigns', 'Adsets', or 'Ads' for logging.
    """
    all_data = []
    
    # Initial parameters for the first request
    params = {'fields': fields, 'limit': 100} # Set a reasonable limit per page
    
    print(f"  -> Starting paged fetch for {entity_name}...")
    
    # Get the iterator object from the SDK
    try:
        iterator = fetch_method(params=params)
    except Exception as e:
        print(f"  🔴 Error starting {entity_name} fetch: {e}")
        raise

    # Iterate through the pages
    page_count = 0
    total_count = 0
    for entity in iterator:
        all_data.append(entity.export_all_data())
        total_count += 1
        
        # Check if we've moved to a new page (SDK handles this internally, 
        # but we use 'iterator.load_next_page()' logic if needed, 
        # or simply check the cursor to determine when the page changes)
        # For simplicity and maximum safety, we sleep after a batch of 100 entities (our limit)
        if total_count % 100 == 0:
            page_count += 1
            print(f"  -> Fetched {total_count} {entity_name} across {page_count} pages. Sleeping for 2s...")
            time.sleep(2) # 🌟 CRITICAL SAFEGUARD

    print(f"  -> Completed fetch for {entity_name}. Total entities: {total_count}")
    return all_data

# --- Fetch Meta Data (MODIFIED: Implements Paging for Structure) ---
def fetch_meta_data(ad_account_id, last_run_time_insight,run_timestamp_dt):
    """
    Fetches structure and insights for a single ad_account_id, using safe paging 
    for structure data and date-based incremental fetching for insights.
    """
    ad_account = AdAccount(ad_account_id)
    data = {}
    
    # --- 1. Fetch Structure Data (Campaigns, Adsets, Ads) using safe paging ---
    
    # Campaigns
    # campaign_fields = ['id', 'name', 'objective', 'status', 'start_time', 'stop_time']
    campaign_fields = ['account_id', 'adlabels', 'advantage_state_info', 'bid_strategy', 'boosted_object_id', 'brand_lift_studies', 'budget_rebalance_flag', 'budget_remaining', 'buying_type', 'campaign_group_active_time', 'can_create_brand_lift_study', 'can_use_spend_cap', 'configured_status', 'created_time', 'daily_budget', 'effective_status', 'has_secondary_skadnetwork_reporting', 'id', 'is_adset_budget_sharing_enabled', 'is_budget_schedule_enabled', 'is_direct_send_campaign', 'is_message_campaign', 'is_skadnetwork_attribution', 'issues_info', 'last_budget_toggling_time', 'lifetime_budget', 'name', 'objective', 'pacing_type', 'primary_attribution', 'promoted_object', 'recommendations', 'smart_promotion_type', 'source_campaign', 'source_campaign_id', 'source_recommendation_type', 'special_ad_categories', 'special_ad_category', 'special_ad_category_country', 'spend_cap', 'start_time', 'status', 'stop_time', 'topline_id', 'updated_time', 'adbatch', 'budget_schedule_specs', 'execution_options', 'iterative_split_test_configs']
    data['campaigns'] = fetch_paged_data_safely(ad_account.get_campaigns, campaign_fields, 'Campaigns')
    
    # Adsets
    # adset_fields = ['id', 'name', 'campaign_id', 'status', 'targeting']
    # adset_fields = ['account_id', 'adlabels', 'adset_schedule', 'asset_feed_id', 'attribution_spec', 'automatic_manual_state', 'bid_adjustments', 'bid_amount', 'bid_constraints', 'bid_info', 'bid_strategy', 'billing_event', 'brand_safety_config', 'budget_remaining', 'campaign', 'campaign_active_time', 'campaign_attribution', 'campaign_id', 'configured_status', 'created_time', 'creative_sequence', 'creative_sequence_repetition_pattern', 'daily_budget', 'daily_min_spend_target', 'daily_spend_cap', 'destination_type', 'dsa_beneficiary', 'dsa_payor', 'effective_status', 'end_time', 'existing_customer_budget_percentage', 'frequency_control_specs', 'full_funnel_exploration_mode', 'id', 'instagram_user_id', 'is_ba_skip_delayed_eligible', 'is_budget_schedule_enabled', 'is_dynamic_creative', 'is_incremental_attribution_enabled', 'issues_info', 'learning_stage_info', 'lifetime_budget', 'lifetime_imps', 'lifetime_min_spend_target', 'lifetime_spend_cap', 'max_budget_spend_percentage', 'min_budget_spend_percentage', 'multi_optimization_goal_weight', 'name', 'optimization_goal', 'optimization_sub_event', 'pacing_type', 'placement_soft_opt_out', 'promoted_object', 'recommendations', 'recurring_budget_semantics', 'regional_regulated_categories', 'regional_regulation_identities', 'review_feedback', 'rf_prediction_id', 'source_adset', 'source_adset_id', 'start_time', 'status', 'targeting', 'targeting_optimization_types', 'time_based_ad_rotation_id_blocks', 'time_based_ad_rotation_intervals', 'trending_topics_spec', 'updated_time', 'use_new_app_click', 'value_rule_set_id', 'value_rules_applied', 'budget_schedule_specs', 'budget_source', 'budget_split_set_id', 'campaign_spec', 'daily_imps', 'date_format', 'execution_options', 'is_sac_cfca_terms_certified', 'line_number', 'rb_prediction_id', 'time_start', 'time_stop', 'topline_id', 'tune_for_category']
    adset_fields = ['id', 'name', 'campaign_id', 'account_id', 'status', 'effective_status', 'configured_status', 'created_time', 'updated_time', 'start_time', 'end_time', 'daily_budget', 'lifetime_budget', 'budget_remaining', 'bid_strategy', 'bid_amount', 'billing_event', 'pacing_type', 'optimization_goal', 'optimization_sub_event', 'learning_stage_info', 'destination_type', 'is_dynamic_creative', 'review_feedback']
    data['adsets'] = fetch_paged_data_safely(ad_account.get_ad_sets, adset_fields, 'Adsets')
    
    # Ads
    # ad_fields = ['id', 'name', 'adset_id', 'campaign_id', 'status', 'creative']
    # ad_fields = ['account_id', 'ad_active_time', 'ad_review_feedback', 'ad_schedule_end_time', 'ad_schedule_start_time', 'adlabels', 'adset', 'adset_id', 'bid_amount', 'bid_info', 'bid_type', 'campaign', 'campaign_id', 'configured_status', 'conversion_domain', 'conversion_specs', 'created_time', 'creative', 'creative_asset_groups_spec', 'demolink_hash', 'display_sequence', 'effective_status', 'engagement_audience', 'failed_delivery_checks', 'id', 'issues_info', 'last_updated_by_app_id', 'name', 'placement', 'preview_shareable_link', 'priority', 'recommendations', 'source_ad', 'source_ad_id', 'status', 'targeting', 'tracking_and_conversion_with_defaults', 'tracking_specs', 'updated_time', 'adset_spec', 'audience_id', 'date_format', 'draft_adgroup_id', 'execution_options', 'include_demolink_hashes', 'filename']
    ad_fields = ['id', 'name', 'adset_id', 'campaign_id', 'status', 'creative', 'account_id', 'effective_status', 'configured_status', 'created_time', 'updated_time', 'bid_amount', 'bid_type', 'call_to_action_type', 'conversion_domain','ad_review_feedback','targeting ','adlabels','issues_info']
    data['ads'] = fetch_paged_data_safely(ad_account.get_ads, ad_fields, 'Ads')


    # --- 2. Time Range for Incremental Insights (Logic is stable) ---
    end_date_str = (run_timestamp_dt - timedelta(days=1)).strftime('%Y-%m-%d') 
    
    if last_run_time_insight is None:
        start_date_dt = (run_timestamp_dt - timedelta(days=7))
    else:
        start_date_dt = last_run_time_insight.date() + timedelta(days=1)
        
    start_date_str = start_date_dt.strftime('%Y-%m-%d')
    
    if start_date_str > end_date_str:
        print(f"  Insights are up-to-date. Range: {start_date_str} to {end_date_str}. Skipping insight fetch.")
        data['ad_insights'] = []
        data['adset_insights'] = []
        data['campaign_insights'] = []
        return data

    print(f"  Insights Time Range: SINCE {start_date_str} UNTIL {end_date_str}")

    time_range_params = {
        'time_range': {
            'since': start_date_str, 
            'until': end_date_str
        },
        'time_increment': 1
    }
    # insight_fields = ['account_currency', 'account_id', 'account_name', 'action_values', 'actions', 'ad_click_actions', 'ad_id', 'ad_impression_actions', 'ad_name', 'adset_end', 'adset_id', 'adset_name', 'adset_start', 'age_targeting', 'attribution_setting', 'auction_bid', 'auction_competitiveness', 'auction_max_competitor_bid', 'average_purchases_conversion_value', 'buying_type', 'campaign_id', 'campaign_name', 'canvas_avg_view_percent', 'canvas_avg_view_time', 'catalog_segment_actions', 'catalog_segment_value', 'catalog_segment_value_mobile_purchase_roas', 'catalog_segment_value_omni_purchase_roas', 'catalog_segment_value_website_purchase_roas', 'clicks', 'conversion_lead_rate', 'conversion_leads', 'conversion_rate_ranking', 'conversion_values', 'conversions', 'converted_product_app_custom_event_fb_mobile_purchase', 'converted_product_app_custom_event_fb_mobile_purchase_value', 'converted_product_offline_purchase', 'converted_product_offline_purchase_value', 'converted_product_omni_purchase', 'converted_product_omni_purchase_values', 'converted_product_quantity', 'converted_product_value', 'converted_product_website_pixel_purchase', 'converted_product_website_pixel_purchase_value', 'converted_promoted_product_app_custom_event_fb_mobile_purchase', 'converted_promoted_product_app_custom_event_fb_mobile_purchase_value', 'converted_promoted_product_offline_purchase', 'converted_promoted_product_offline_purchase_value', 'converted_promoted_product_omni_purchase', 'converted_promoted_product_omni_purchase_values', 'converted_promoted_product_quantity', 'converted_promoted_product_value', 'converted_promoted_product_website_pixel_purchase', 'converted_promoted_product_website_pixel_purchase_value', 'cost_per_15_sec_video_view', 'cost_per_2_sec_continuous_video_view', 'cost_per_action_type', 'cost_per_ad_click', 'cost_per_conversion', 'cost_per_conversion_lead', 'cost_per_dda_countby_convs', 'cost_per_estimated_ad_recallers', 'cost_per_inline_link_click', 'cost_per_inline_post_engagement', 'cost_per_objective_result', 'cost_per_one_thousand_ad_impression', 'cost_per_outbound_click', 'cost_per_result', 'cost_per_thruplay', 'cost_per_unique_action_type', 'cost_per_unique_click', 'cost_per_unique_conversion', 'cost_per_unique_inline_link_click', 'cost_per_unique_outbound_click', 'cpc', 'cpm', 'cpp', 'created_time', 'creative_media_type', 'ctr', 'date_start', 'date_stop', 'dda_countby_convs', 'dda_results', 'engagement_rate_ranking', 'estimated_ad_recall_rate', 'estimated_ad_recall_rate_lower_bound', 'estimated_ad_recall_rate_upper_bound', 'estimated_ad_recallers', 'estimated_ad_recallers_lower_bound', 'estimated_ad_recallers_upper_bound', 'frequency', 'full_view_impressions', 'full_view_reach', 'gender_targeting', 'impressions', 'inline_link_click_ctr', 'inline_link_clicks', 'inline_post_engagement', 'instagram_upcoming_event_reminders_set', 'instant_experience_clicks_to_open', 'instant_experience_clicks_to_start', 'instant_experience_outbound_clicks', 'interactive_component_tap', 'labels', 'landing_page_view_actions_per_link_click', 'landing_page_view_per_link_click', 'landing_page_view_per_purchase_rate', 'link_clicks_per_results', 'location', 'marketing_messages_click_rate_benchmark', 'marketing_messages_cost_per_delivered', 'marketing_messages_cost_per_link_btn_click', 'marketing_messages_delivered', 'marketing_messages_delivery_rate', 'marketing_messages_link_btn_click', 'marketing_messages_link_btn_click_rate', 'marketing_messages_media_view_rate', 'marketing_messages_phone_call_btn_click_rate', 'marketing_messages_quick_reply_btn_click', 'marketing_messages_quick_reply_btn_click_rate', 'marketing_messages_read', 'marketing_messages_read_rate', 'marketing_messages_read_rate_benchmark', 'marketing_messages_sent', 'marketing_messages_spend', 'marketing_messages_spend_currency', 'marketing_messages_website_add_to_cart', 'marketing_messages_website_initiate_checkout', 'marketing_messages_website_purchase', 'marketing_messages_website_purchase_values', 'mobile_app_purchase_roas', 'objective', 'objective_result_rate', 'objective_results', 'onsite_conversion_messaging_detected_purchase_deduped', 'optimization_goal', 'outbound_clicks', 'outbound_clicks_ctr', 'place_page_name', 'product_group_retailer_id', 'product_retailer_id', 'product_views', 'purchase_per_landing_page_view', 'purchase_roas', 'purchases_per_link_click', 'qualifying_question_qualify_answer_rate', 'quality_ranking', 'reach', 'result_rate', 'result_values_performance_indicator', 'results', 'shops_assisted_purchases', 'social_spend', 'spend', 'total_card_view', 'total_postbacks', 'total_postbacks_detailed', 'total_postbacks_detailed_v4', 'unique_actions', 'unique_clicks', 'unique_conversions', 'unique_ctr', 'unique_inline_link_click_ctr', 'unique_inline_link_clicks', 'unique_link_clicks_ctr', 'unique_outbound_clicks', 'unique_outbound_clicks_ctr', 'unique_video_continuous_2_sec_watched_actions', 'unique_video_view_15_sec', 'updated_time', 'video_15_sec_watched_actions', 'video_30_sec_watched_actions', 'video_avg_time_watched_actions', 'video_continuous_2_sec_watched_actions', 'video_p100_watched_actions', 'video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions', 'video_p95_watched_actions', 'video_play_actions', 'video_play_curve_actions', 'video_play_retention_0_to_15s_actions', 'video_play_retention_20_to_60s_actions', 'video_play_retention_graph_actions', 'video_thruplay_watched_actions', 'video_time_watched_actions', 'video_view_per_impression', 'website_ctr', 'website_purchase_roas', 'wish_bid']
    # insight_fields = ['account_currency', 'account_id', 'account_name', 'action_values', 'actions','ad_click_actions', 'ad_id', 'ad_impression_actions', 'ad_name', 'adset_end','adset_id', 'adset_name', 'adset_start', 'attribution_setting', 'auction_bid','auction_competitiveness', 'auction_max_competitor_bid', 'average_purchases_conversion_value','buying_type', 'campaign_id', 'campaign_name', 'canvas_avg_view_percent','canvas_avg_view_time', 'catalog_segment_actions', 'catalog_segment_value','catalog_segment_value_mobile_purchase_roas', 'catalog_segment_value_omni_purchase_roas','catalog_segment_value_website_purchase_roas', 'clicks', 'conversion_lead_rate','conversion_leads', 'conversion_rate_ranking', 'conversion_values', 'conversions','cost_per_15_sec_video_view', 'cost_per_2_sec_continuous_video_view','cost_per_action_type', 'cost_per_ad_click', 'cost_per_conversion', 'cost_per_conversion_lead','cost_per_dda_countby_convs', 'cost_per_estimated_ad_recallers','cost_per_inline_link_click', 'cost_per_inline_post_engagement', 'cost_per_objective_result','cost_per_one_thousand_ad_impression', 'cost_per_outbound_click', 'cost_per_result','cost_per_thruplay', 'cost_per_unique_action_type', 'cost_per_unique_click','cost_per_unique_conversion', 'cost_per_unique_inline_link_click', 'cost_per_unique_outbound_click','cpc', 'cpm', 'cpp', 'created_time', 'creative_media_type', 'ctr', 'date_start','date_stop', 'dda_countby_convs', 'dda_results', 'engagement_rate_ranking','estimated_ad_recall_rate', 'estimated_ad_recallers', 'frequency', 'full_view_impressions','full_view_reach', 'impressions', 'inline_link_click_ctr', 'inline_link_clicks','inline_post_engagement', 'instagram_upcoming_event_reminders_set','instant_experience_clicks_to_open', 'instant_experience_clicks_to_start','instant_experience_outbound_clicks', 'interactive_component_tap','landing_page_view_actions_per_link_click', 'landing_page_view_per_link_click','landing_page_view_per_purchase_rate', 'link_clicks_per_results','marketing_messages_click_rate_benchmark', 'marketing_messages_cost_per_delivered','marketing_messages_cost_per_link_btn_click', 'marketing_messages_delivered','marketing_messages_delivery_rate', 'marketing_messages_link_btn_click','marketing_messages_link_btn_click_rate', 'marketing_messages_media_view_rate','marketing_messages_phone_call_btn_click_rate', 'marketing_messages_quick_reply_btn_click','marketing_messages_quick_reply_btn_click_rate', 'marketing_messages_read','marketing_messages_read_rate', 'marketing_messages_read_rate_benchmark','marketing_messages_sent', 'marketing_messages_spend', 'marketing_messages_spend_currency','marketing_messages_website_add_to_cart', 'marketing_messages_website_initiate_checkout','marketing_messages_website_purchase', 'marketing_messages_website_purchase_values','mobile_app_purchase_roas', 'objective', 'objective_result_rate', 'objective_results','onsite_conversion_messaging_detected_purchase_deduped', 'optimization_goal','outbound_clicks', 'outbound_clicks_ctr', 'place_page_name', 'product_group_retailer_id','product_retailer_id', 'product_views', 'purchase_per_landing_page_view', 'purchase_roas','purchases_per_link_click', 'qualifying_question_qualify_answer_rate', 'quality_ranking','reach', 'result_rate', 'result_values_performance_indicator', 'results','shops_assisted_purchases', 'social_spend', 'spend', 'total_card_view','unique_actions', 'unique_clicks', 'unique_conversions', 'unique_ctr','unique_inline_link_click_ctr', 'unique_inline_link_clicks', 'unique_link_clicks_ctr','unique_outbound_clicks', 'unique_outbound_clicks_ctr', 'unique_video_continuous_2_sec_watched_actions','unique_video_view_15_sec', 'updated_time', 'video_15_sec_watched_actions','video_30_sec_watched_actions', 'video_avg_time_watched_actions','video_continuous_2_sec_watched_actions', 'video_p100_watched_actions','video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions','video_p95_watched_actions', 'video_play_actions', 'video_play_curve_actions','video_play_retention_0_to_15s_actions', 'video_play_retention_20_to_60s_actions','video_play_retention_graph_actions', 'video_thruplay_watched_actions','video_time_watched_actions', 'video_view_per_impression', 'website_ctr','website_purchase_roas', 'wish_bid']
    insight_fields = [
    'account_currency', 'account_id', 'account_name', 'ad_id', 'ad_name', 
    'adset_id', 'adset_name', 'campaign_id', 'campaign_name', 'buying_type', 
    'objective', 'date_start', 'date_stop', 
    
    # Core Metrics
    'impressions', 'reach', 'frequency', 'spend', 'social_spend', 
    
    # Clicks & CTR
    'clicks', 'cpc', 'cpm', 'ctr', 'unique_clicks', 'unique_ctr', 
    'inline_link_clicks', 'inline_link_click_ctr', 'unique_inline_link_clicks', 
    'unique_inline_link_click_ctr', 'outbound_clicks', 'outbound_clicks_ctr', 
    'unique_outbound_clicks', 'unique_outbound_clicks_ctr',

    # Post Engagement
    'inline_post_engagement', 'cost_per_inline_post_engagement', 

    # Results & Conversions (Summary fields)
    'results', 'result_rate', 'cost_per_result', 'optimization_goal',
    'conversions', 'conversion_values', 'cost_per_conversion', 
    'actions', 'action_values', 'unique_actions', 'cost_per_action_type', 
    'cost_per_unique_action_type',

    # Video & Canvas 
    'video_thruplay_watched_actions',
    'canvas_avg_view_percent', 'canvas_avg_view_time',

    # Quality and Times
    'quality_ranking', 'conversion_rate_ranking', 'engagement_rate_ranking', 
    'created_time', 'updated_time',]
   
    breakdown_params = {'breakdowns': ['publisher_platform', 'platform_position']}
    # Ensure the limit is present for insights as well, though the SDK handles insight paging
    limit_param = {'limit': 1000} 
    
    print("  -> Fetching Insights...")

    # Fetch Insights (The SDK handles insight paging robustly, so we don't need the manual sleep loop here)
    data['ad_insights'] = [i.export_all_data() for i in ad_account.get_insights(
        fields=insight_fields + ['campaign_id', 'adset_id', 'ad_id'],
        params={**time_range_params, 'level': 'ad', **breakdown_params, **limit_param}
    )]
    data['adset_insights'] = [i.export_all_data() for i in ad_account.get_insights(
        fields=insight_fields + ['campaign_id', 'adset_id'],
        params={**time_range_params, 'level': 'adset', **breakdown_params, **limit_param}
    )]
    data['campaign_insights'] = [i.export_all_data() for i in ad_account.get_insights(
        fields=insight_fields + ['campaign_id'],
        params={**time_range_params, 'level': 'campaign', **breakdown_params, **limit_param}
    )]
    
    total_insights = len(data['ad_insights']) + len(data['adset_insights']) + len(data['campaign_insights'])

    print(f"Fetched structure for {ad_account_id}: {len(data['campaigns'])} campaigns, {len(data['adsets'])} adsets, {len(data['ads'])} ads. Total Insights: {total_insights}")
    return data

# --- BigQuery Load Function (Kept the same) ---
def load_data_to_bigquery(client, df, table_name):
    """Loads a pandas DataFrame into BigQuery."""
    full_table_id = f"{client.project}.{BQ_CONFIG['dataset_id']}.{table_name}"
    
    is_insight_table = table_name in ['ad_insights', 'adset_insights', 'campaign_insights', 'insights']
    
    # Truncate structure tables, Append to insight tables (due to time partitioning)
    write_disp = bigquery.WriteDisposition.WRITE_TRUNCATE if not is_insight_table else bigquery.WriteDisposition.WRITE_APPEND
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disp,
        time_partitioning=bigquery.TimePartitioning(
            field="last_run_timestamp",
            type_=bigquery.TimePartitioningType.DAY
        ) if is_insight_table else None
    )

    print(f"🚀 Starting load job for table: {full_table_id}")
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    print(f"🎉 Successfully loaded {len(df)} rows to BigQuery table: {full_table_id}")

# --- Audit Log (Kept the same) ---
def log_audit_entry(client, run_timestamp, ad_account_id, table_name, rows_processed, status, error_message=None):
    """Loads a single log entry into the dedicated ETL Audit Log table using pandas-gbq."""
    
    audit_table_name = 'etl_audit_log'
    full_table_id = f"{BQ_CONFIG['dataset_id']}.{audit_table_name}"

    log_data = [{
        'run_timestamp': run_timestamp,  
        'table_name': table_name,
        'ad_account_id': ad_account_id, 
        'rows_processed': rows_processed,
        'status': status,
        'error_message': str(error_message) if error_message else None 
    }]
    
    log_df = pd.DataFrame(log_data)
    
    log_schema = [
        {'name': 'run_timestamp', 'data_type': 'TIMESTAMP'},
        {'name': 'table_name', 'data_type': 'STRING'},
        {'name': 'ad_account_id', 'data_type': 'STRING'},
        {'name': 'rows_processed', 'data_type': 'INTEGER'},
        {'name': 'status', 'data_type': 'STRING'},
        {'name': 'error_message', 'data_type': 'STRING'},
    ]

    try:
        pandas_gbq.to_gbq(
           dataframe= log_df,
            destination_table=full_table_id,
            project_id=BQ_CONFIG['project_id'],
            if_exists='append',
            table_schema=log_schema
        )
        print(f"✅ Logged audit entry for {table_name} using pandas-gbq.")
    except Exception as e:
        sys.stderr.write(f"🔴 FATAL ERROR: Failed to log audit entry for {table_name}. Error: {e}\n")

# --- Last Run Time (Kept the same) ---
def last_run_time_for_insight(client, ad_account_id):
    """Finds the max run_timestamp for a specific ad_account_id."""
    table_name = 'etl_audit_log'
    full_table_id = f"{client.project}.{BQ_CONFIG['dataset_id']}.{table_name}"
    
    try:
        query = f"""
            SELECT MAX(run_timestamp) AS last_run_timestamp
            FROM `{full_table_id}`
            WHERE ad_account_id = @ad_account_id
              AND table_name IN ('ad_insights', 'adset_insights', 'campaign_insights')
              AND status = 'SUCCESS'
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("ad_account_id", "STRING", ad_account_id)
            ]
        )
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()
        for row in results:
            return row.last_run_timestamp
    except Exception as e:
        print(f"Warning: Could not query audit log for last run time for {ad_account_id}. Assuming first run. Error: {e}")
        return None

# --- Main Execution (Stricter Rate Limit Handling Maintained) ---
def main():
    bq_client = initialize_bigquery_client()
    initialize_meta_api()
    
    # 1. Ensure BQ infrastructure is ready
    create_bigquery_dataset(bq_client)
    ensure_audit_log_table(bq_client)

    run_timestamp_dt = datetime.now()
    
    # 2. Outer loop for all Ad Accounts
    for ad_account_id in META_CONFIG['ad_account_ids']:
        print(f"\n--- Starting ETL for Ad Account: {ad_account_id} ---")
        
        last_run_time_insight = last_run_time_for_insight(bq_client, ad_account_id)

        # 3. Fetch Data
        try:
            # THIS CALL NOW USES THE NEW PAGED FETCH LOGIC
            meta_data = fetch_meta_data(ad_account_id, last_run_time_insight,run_timestamp_dt)
            meta_data_py = json.loads(json.dumps(meta_data))
        
        except Exception as e:
            error_message = str(e)
            log_audit_entry(bq_client, run_timestamp_dt, ad_account_id, "FETCH_ALL", 0, "FATAL_FAILURE", error_message)
            print(f"🔴 Fatal Error during data fetch for {ad_account_id}: {e}")
            
            # 🌟 REVISED FAILURE BACKOFF 🌟
            if "User request limit reached" in error_message:
                backoff_time = 180 
                print(f"🛑 RATE LIMIT HIT. Resting for a long backoff of {backoff_time} seconds before the next account...")
                time.sleep(backoff_time)
            else:
                backoff_time = 30
                print(f"😴 Resting for {backoff_time} seconds after fatal error...")
                time.sleep(backoff_time)
            
            continue # Skip to the next account
            

        # 4. Create and Prepare DataFrames
        campaigns = pd.DataFrame(meta_data_py.get('campaigns', []))
        adsets = pd.DataFrame(meta_data_py.get('adsets', []))
        ads = pd.DataFrame(meta_data_py.get('ads', []))
        ad_insights = pd.DataFrame(meta_data_py.get('ad_insights', []))
        adset_insights = pd.DataFrame(meta_data_py.get('adset_insights', []))
        campaign_insights = pd.DataFrame(meta_data_py.get('campaign_insights', []))

        # Add the ad_account_id and audit timestamp column to ALL DataFrames
        all_dfs = [campaigns, adsets, ads, ad_insights, adset_insights, campaign_insights]
        for df in all_dfs:
            if not df.empty:
                df['ad_account_id'] = ad_account_id
                df['last_run_timestamp'] = run_timestamp_dt
                
        # Convert 'date_start' for insights to datetime objects
        insight_dfs = [ad_insights, adset_insights, campaign_insights]
        for df in insight_dfs:
            if not df.empty and 'date_start' in df.columns:
                df['date_start'] = pd.to_datetime(df['date_start'])
                
        # 5. Define tables to load
        dataframes_to_load = {
            'campaigns': campaigns,          
            'adsets': adsets,                
            'ads': ads,                      
            'ad_insights': ad_insights,      
            'adset_insights': adset_insights,  
            'campaign_insights': campaign_insights 
        }

        # 6. Load DataFrames to BigQuery with Logging
        for table_name, df in dataframes_to_load.items():
            rows_processed = len(df)
            
            if rows_processed == 0:
                print(f"Skipping {table_name} for {ad_account_id}: DataFrame is empty.")
                log_audit_entry(bq_client, run_timestamp_dt, ad_account_id, table_name, 0, "SKIPPED")
                continue
                
            try:
                load_data_to_bigquery(bq_client, df, table_name) 
                log_audit_entry(bq_client, run_timestamp_dt, ad_account_id, table_name, rows_processed, "SUCCESS")

            except Exception as e:
                print(f"❌ An error occurred while loading {table_name} for {ad_account_id} to BigQuery: {e}")
                log_audit_entry(bq_client, run_timestamp_dt, ad_account_id, table_name, rows_processed, "FAILURE", str(e))

        # 7. Respect Meta API Rate Limits
        success_rest_time = 30 # Increased standard rest time between successful accounts
        print(f"😴 Resting for {success_rest_time} seconds to respect Meta API limits...")
        time.sleep(success_rest_time)
        
if __name__ == "__main__":
    main()
