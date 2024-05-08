from fastapi import FastAPI
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta

app = FastAPI()
client = bigquery.Client()

@app.get("/update_campaign_data")
def update_campaign_data(no_days_to_back_fill: int = 10):


    # Define the query
    query_1 = f"""
    SELECT distinct campaign_id
    FROM `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_CampaignBasicStats_2222881946`
    WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {no_days_to_back_fill} DAY)
    """

    # Run the query
    query_job = client.query(query_1)

    df_1 = query_job.to_dataframe()

    query_2 = """
    SELECT campaign_id, campaign_name FROM ga4-bq-connector.DemoData.campaign_mapping
    """

    # Run the query
    query_job = client.query(query_2)

    # Get the results as a pandas DataFrame
    df_2 = query_job.to_dataframe()

    missing_campaign_ids = df_1[~df_1['campaign_id'].isin(df_2['campaign_id'])]

    # Extract the number from each campaign name
    df_2['campaign_number'] = df_2['campaign_name'].str.extract('(\d+)', expand=False).astype(int)

    # If the campaign_number column is still not present (all values are NaN), set highest_number to 1
    if 'campaign_number' not in df_2 or df_2['campaign_number'].isna().all():
        highest_number = 1
    else:
        # Find the highest number
        highest_number = df_2['campaign_number'].max()

    # Count the number of missing campaign_ids
    missing_count = len(missing_campaign_ids)

    # Create a list of numbers starting from highest_number + 1
    numbers = range(highest_number + 1, highest_number + 1 + missing_count)

    # Append "campaign_name_" to each number
    campaign_names = ["campaign_name_" + str(number) for number in numbers]
    new_campaign_ids = ["c_id_" + str(number) for number in numbers]

    # Pair each missing campaign_id with a campaign_name and new_campaign_id
    pairs = list(zip(missing_campaign_ids['campaign_id'], campaign_names, new_campaign_ids))

    # Create a DataFrame from the pairs list
    df_pairs = pd.DataFrame(pairs, columns=['campaign_id', 'campaign_name', 'new_campaign_id'])

    # Convert int64 columns to string
    for col in df_pairs.columns:
        if df_pairs[col].dtype == 'int64':
            df_pairs[col] = df_pairs[col].astype(str)

    # Define the table
    table = 'ga4-bq-connector.DemoData.campaign_mapping'

    # Fetch the existing campaign_ids from the BigQuery table
    query = """
    SELECT campaign_id FROM `ga4-bq-connector.DemoData.campaign_mapping`
    """
    existing_campaign_ids = pd.read_gbq(query, project_id=client.project)['campaign_id']

    # Filter df_pairs to only include rows with campaign_ids that are not already in the table
    df_pairs = df_pairs[~df_pairs['campaign_id'].isin(existing_campaign_ids)]

    # Now try to append to the BigQuery table
    pandas_gbq.to_gbq(df_pairs, table, client.project, if_exists='append')

    query_3 = f"""
    SELECT 
         cm.new_campaign_id as campaign_id, 
        cm.campaign_name as campaign_name,
        p.metrics_clicks, 
        p.metrics_conversions, 
        p.metrics_conversions_value, 
        p.metrics_cost_micros, 
        p.metrics_impressions, 
        p.metrics_interaction_event_types, 
        p.metrics_interactions, 
        p.metrics_view_through_conversions, 
        p.segments_ad_network_type, 
        p.segments_date, 
        p.segments_device, 
        p.segments_slot
    FROM `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_CampaignBasicStats_2222881946` AS p
    LEFT JOIN `ga4-bq-connector.DemoData.campaign_mapping` AS cm
    ON CAST(p.campaign_id AS STRING) = cm.campaign_id
    WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {no_days_to_back_fill} DAY)
    """

    # Run the query
    query_job = client.query(query_3)

    # Get the result as a pandas DataFrame
    df = query_job.to_dataframe()

    # Define the destination table
    table_id = 'ga4-bq-connector.DemoData.p_ads_CampaignBasicStats'

    # Query the existing data from the BigQuery table
    sql = f"SELECT * FROM `{table_id}`"
    query_job = client.query(sql)
    existing_df = query_job.to_dataframe()

    # Remove any rows from df that are already in the existing_df
    df = pd.concat([df, existing_df, existing_df]).drop_duplicates(keep=False)

    # Append the data to the table
    pandas_gbq.to_gbq(df, table_id, client.project, if_exists='append')

    # Return the number of rows that will be added
    return {"Number of rows added": len(df)}

#########Anonymize Ad Data################
@app.get("/update_ad_date")
def update_ad_date(no_days_to_back_fill: int = 28):
    

    sql_query_1 = f"""
    SELECT distinct campaign_id
    FROM `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_AdBasicStats_2222881946`
    WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {no_days_to_back_fill} DAY)
    """
    query_job = client.query(sql_query_1)

    df_1 = query_job.to_dataframe()

    query_2 = """
    SELECT campaign_id, campaign_name FROM ga4-bq-connector.DemoData.campaign_mapping
    """

    # Run the query
    query_job = client.query(query_2)

    # Get the results as a pandas DataFrame
    df_2 = query_job.to_dataframe()

    missing_campaign_ids = df_1[~df_1['campaign_id'].isin(df_2['campaign_id'])]

    # Extract the number from each campaign name
    df_2['campaign_number'] = df_2['campaign_name'].str.extract('(\d+)', expand=False).astype(int)

    # If the campaign_number column is still not present (all values are NaN), set highest_number to 1
    if 'campaign_number' not in df_2 or df_2['campaign_number'].isna().all():
        highest_number = 1
    else:
        # Find the highest number
        highest_number = df_2['campaign_number'].max()

    # Count the number of missing campaign_ids
    missing_count = len(missing_campaign_ids)

    # Create a list of numbers starting from highest_number + 1
    numbers = range(highest_number + 1, highest_number + 1 + missing_count)

    # Append "campaign_name_" to each number
    campaign_names = ["campaign_name_" + str(number) for number in numbers]
    new_campaign_ids = ["c_id_" + str(number) for number in numbers]

    # Pair each missing campaign_id with a campaign_name and new_campaign_id
    pairs = list(zip(missing_campaign_ids['campaign_id'], campaign_names, new_campaign_ids))

    # Create a DataFrame from the pairs list
    df_pairs = pd.DataFrame(pairs, columns=['campaign_id', 'campaign_name', 'new_campaign_id'])

    # Convert int64 columns to string
    for col in df_pairs.columns:
        if df_pairs[col].dtype == 'int64':
            df_pairs[col] = df_pairs[col].astype(str)

    # Define the table
    table = 'ga4-bq-connector.DemoData.campaign_mapping'

    # Fetch the existing campaign_ids from the BigQuery table
    query = """
    SELECT campaign_id FROM `ga4-bq-connector.DemoData.campaign_mapping`
    """
    existing_campaign_ids = pd.read_gbq(query, project_id=client.project)['campaign_id']

    # Filter df_pairs to only include rows with campaign_ids that are not already in the table
    df_pairs = df_pairs[~df_pairs['campaign_id'].isin(existing_campaign_ids)]

    # Now try to append to the BigQuery table
    pandas_gbq.to_gbq(df_pairs, table, client.project, if_exists='append')

    query_3 = f"""
    SELECT 
  p.ad_group_ad_ad_id, 
  p.ad_group_id,  
  cm.new_campaign_id as campaign_id, 
  p.customer_id, 
  p.ad_group_ad_ad_group, 
  p.ad_group_base_ad_group, 
  p.campaign_base_campaign, 
  p.metrics_clicks, 
  p.metrics_conversions, 
  p.metrics_conversions_value, 
  p.metrics_cost_micros, 
  p.metrics_impressions, 
  p.metrics_interaction_event_types, 
  p.metrics_interactions, 
  p.metrics_view_through_conversions, 
  p.segments_ad_network_type, 
  p.segments_date, 
  p.segments_device, 
  p.segments_slot
FROM 
  ga4-bq-connector.TDKGoogleAdsDataset.p_ads_AdBasicStats_2222881946 AS p 
LEFT JOIN 
  ga4-bq-connector.DemoData.campaign_mapping AS cm 
ON 
  CAST(p.campaign_id AS STRING) = cm.campaign_id 
WHERE 
  p.segments_date >= DATE_SUB(
    CURRENT_DATE(), 
    INTERVAL {no_days_to_back_fill} DAY
  )
    """

    # Run the query
    query_job = client.query(query_3)

    # Get the result as a pandas DataFrame
    df = query_job.to_dataframe()

    # Define the destination table
    table_id = 'ga4-bq-connector.DemoData.p_ads_AdBasicStats'

    # Query the existing data from the BigQuery table
    sql = f"SELECT * FROM `{table_id}`"
    query_job = client.query(sql)
    existing_df = query_job.to_dataframe()

    # Remove any rows from df that are already in the existing_df
    df = pd.concat([df, existing_df, existing_df]).drop_duplicates(keep=False)

    # Append the data to the table
    pandas_gbq.to_gbq(df, table_id, client.project, if_exists='append')

    # Return the number of rows that will be added
    return {"Number of rows added": len(df)}

