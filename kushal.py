# Python

from google.cloud import bigquery
import os
import pandas as pd
import numpy as np
import random


from fastapi import FastAPI, UploadFile, File

from typing import List


from sklearn.model_selection import train_test_split
from datetime import datetime, timedelta




#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "CustomGPTDemo/service-account-key.json"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account-key.json"

import logging

# Create a logger
logger = logging.getLogger('my_logger')

# Set the level of the logger. This can be DEBUG, INFO, WARNING, ERROR, or CRITICAL
logger.setLevel(logging.INFO)

# Create a file handler
handler = logging.FileHandler('/tmp/my_log.log')

# Create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the logger
logger.addHandler(handler)


app_k = FastAPI()
client = bigquery.Client.from_service_account_json(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
    project='ga4-bq-connector'
)

@app_k.post("/increase_bidrate_keywords")
def increase_bidrate_keywords():
    query = """
    SELECT *
    FROM `ga4-bq-connector.DemoData.keywords_data_demo2`
    """
    query_job = client.query(query)  # API request
    df = query_job.to_dataframe()  # Converts the results to a pandas DataFrame
    #print(df.head())

    # Convert the 'search_impressions' column to float
    df['search_impressions'] = df['search_impressions'].astype(float)

    # Filter the DataFrame based on the conditions
    df_filtered = df[
        (df['search_impressions'] <= 0.5) &
        (df['CTR'] >=0.1) &
        (df['Conv__rate'] >= 0.1)
    ]
    #print(df_filtered.head())

    # Return the 'keywords' column as a list
    return df_filtered['Keyword'].tolist()



@app_k.post("/keywords_and_metrics")
def keywords_and_metrics_for_experiment():
    # Replace this with the actual code to get the keywords
    keywords = list(set(increase_bidrate_keywords()))
    #print(keywords)

    # Randomly shuffle the list
    random.shuffle(keywords)
    #print(len(keywords))


    # Ask the user for the metrics they want to test
    #print("Suggested metrics: Search impressions, Click-Through Rate (CTR), Conversion ratio, Cost-per-click (CPC), Total cost, Total conversions")
    metrics = ["Search impressions", "Click-Through Rate (CTR)", "Conversion ratio", "Cost-per-click (CPC)", "Total cost", "Total conversions"]
    

    # Find the index to split the list
    split_index = len(keywords) // 2

    # Divide the list into two nearly equal parts
    control = keywords[:split_index]
    test = keywords[split_index:]

    return {"control": control, "test": test, "metrics": metrics}


# control, test, metrics = keywords_and_metrics_for_experiment()["control"], keywords_and_metrics_for_experiment()["test"], keywords_and_metrics_for_experiment()["metrics"]
# print("Control group is:", control)
# print("Test group is:", test)
# print("Metrics:", metrics)

#print(increase_bidrate_keywords())



click_threshold = 50
conversion_threshold = 5
ctr_threshold = 0.5  # Adjusted CTR threshold
conversion_rate_threshold = 2.0
# Undesired terms in the negative keywords list
undesired_terms = ['chemical', 'artificial', 'synthetic']


@app_k.post("/identify_negative_keywords")
def identify_negative_keywords():
    ctr_threshold = 0.05
    conversion_rate_threshold = 0.2

    # Replace with your BigQuery table reference
    table_ref = 'ga4-bq-connector.DemoData.keywords_negative_demo_data2'

    sql_query = f"""
        SELECT 
            KeywordID,
            KeywordText,
            SUM(Clicks) as Clicks,
            SUM(Impressions) as Impressions,
            SUM(Conversion) as Conversion,
            SUM(Cost) as Cost,
            Currency,
            SUM(Clicks)/SUM(Impressions) as CTR,
            SUM(Conversion)/SUM(Clicks) as Conversion_rate,
            Campaign_id,
            Campaign_name
        FROM `{table_ref}`
        GROUP BY 
            KeywordID,
            KeywordText,
            Currency,
            Campaign_id,
            Campaign_name
    """
    query_job = client.query(sql_query)  # API request
    df = query_job.to_dataframe()  # Converts the results to a pandas DataFrame

    # Add additional undesired terms to the potential negative keywords list
    undesired_terms = ['chemical', 'artificial', 'synthetic']

    # Create a new column 'Reason' based on undesired terms, CTR, and Conversion_rate
    df['Reason'] = df.apply(lambda row: 'Undesired Term'
                             if any(term in row['KeywordText'].lower() for term in undesired_terms)
                             else 'Potential Negative Keyword: High CTR, Low Conversions'
                             if row['CTR'] > ctr_threshold and row['Conversion_rate'] < conversion_rate_threshold
                             else 'Performing Well', axis=1)

    # Create two dataframes: one for keywords performing well and one for keywords not performing well
    df_performing_well = df.loc[df['Reason'] == 'Performing Well']
    df_not_performing_well = df.loc[df['Reason'] != 'Performing Well']

    # Convert both dataframes to dictionaries and return them with keys
    return {'performing_well': df_performing_well.to_dict(), 'potential_negative_keyowrds': df_not_performing_well.to_dict()}


@app_k.post("/optimize_campaigns_on_roas_for_last_3_weeks")
def optimize_campaigns_on_roas():
    # Define the SQL query
    sql_query = """
        SELECT campaign_id,
        campaign_name,
        SUM(metrics_conversions_value) AS revenue,
        SUM(metrics_cost_micros)/1000000 AS cost 
FROM `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats_2222881946`
WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
group by campaign_id,campaign_name;
    """

    # Run the query
    query_job = client.query(sql_query)

    # Convert the result to a DataFrame
    campaign_df = query_job.to_dataframe()

    # Calculate the ROAS
    campaign_df['ROAS'] = campaign_df['revenue'] / (campaign_df['cost'])

    # Create the 'stop' DataFrame
    stop_df = campaign_df.loc[campaign_df['ROAS'] < 1]
    stop_df['Reasoning'] = 'Low ROAS. '
    stop_df.loc[stop_df['ROAS'] < 1, 'Reasoning'] += 'ROAS < 1. '

    # Create the 'continue' DataFrame
    continue_df = campaign_df.loc[campaign_df['ROAS'] > 2]
    continue_df['Reasoning'] = 'High ROAS. '
    continue_df.loc[continue_df['ROAS'] > 2, 'Reasoning'] += 'ROAS > 2. '

    # Create the 'optimise' DataFrame
    optimise_df = campaign_df.loc[(campaign_df['ROAS'] >= 1) & (campaign_df['ROAS'] <= 2)]
    optimise_df['Reasoning'] = 'Moderate ROAS. '
    optimise_df.loc[(optimise_df['ROAS'] >= 1) & (optimise_df['ROAS'] <= 2), 'Reasoning'] += '1 <= ROAS <= 2. '

    # Return the dataframes as dictionaries within a dictionary
    return {
        'stop': stop_df.to_dict(),
        'continue': continue_df.to_dict(),
        'optimise': optimise_df.to_dict()
    }

@app_k.post("/get_campaigns_based_on_roas")
def get_campaigns_based_on_roas():
    # Define the SQL query
    sql_query = """
    SELECT
        campaign_id,
        SUM(metrics_conversions_value) AS revenue,
        SUM(metrics_cost_micros)/1000000 AS cost
    FROM `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats_2222881946`
    WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
    GROUP BY campaign_id;
    """

    # Run the query
    query_job = client.query(sql_query)

    # Convert the result to a DataFrame
    campaign_df = query_job.to_dataframe()

    # Calculate the ROAS
    campaign_df['ROAS'] = campaign_df['revenue'] / campaign_df['cost']

    return campaign_df.to_dict()

@app_k.post("/campaign_evaluation_based_on_budget")
def campaign_evaluation_based_on_budget():
    sql_query = """SELECT 
    campaign_id, 
    SUM(metrics_clicks) as clicks,
    SUM(metrics_conversions) as conversions, 
    SUM(metrics_cost_micros)/100000000 as cost,
    SUM(metrics_impressions) as impressions,
    sum(metrics_conversions_value)/10 as conversion_value   
FROM 
    `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats_2222881946` 
WHERE 
    segments_date > DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
GROUP BY 
    campaign_id;"""
    # Run the query
    query_job = client.query(sql_query)

    # Convert the result to a DataFrame
    campaign_df = query_job.to_dataframe()

    # Calculate CPA, CTR, and ROAS
    campaign_df['CPA'] = campaign_df['cost'] / campaign_df['conversions']
    campaign_df['CTR'] = campaign_df['clicks'] / campaign_df['impressions']    
    campaign_df['ROAS'] = campaign_df['conversion_value'] / campaign_df['cost']

    
    

    ## Handle potential division by zero
    campaign_df['CPA'] = campaign_df['CPA'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['CTR'] = campaign_df['CTR'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['ROAS'] = campaign_df['ROAS'].replace([np.inf, -np.inf], np.nan).fillna(0)

    campaign_df['CPA'] = campaign_df['CPA'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['CTR'] = campaign_df['CTR'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['ROAS'] = campaign_df['ROAS'].replace([np.inf, -np.inf], np.nan).fillna(0)

    def apply_rules(row):
        # High-Performance Scenario
        if row['ROAS'] > 1.5 and row['CPA'] < 300 and row['CTR'] > 0.025:
            return ("High-Performance Scenario", "ROAS > 1.5, CPA < 300, CTR > 0.025")
        
        # Moderate-Performance Scenario
        elif 1 <= row['ROAS'] <= 1.5 and 300 <= row['CPA'] <= 700 and 0.01 <= row['CTR'] <= 0.025:
            return ("Moderate-Performance Scenario", "1 <= ROAS <= 1.5, 300 <= CPA <= 700, 0.01 <= CTR <= 0.025")
        
        # Optimization Opportunity Scenario
        elif row['CTR'] > 0.025 and row['CPA'] < 300 and row['ROAS'] >= 1:
            return ("Optimization Opportunity Scenario", "CTR > 0.025, CPA < 300, ROAS >= 1")
        
        # Cost-Effective Scenario
        elif row['CPA'] < 300 and row['CTR'] > 0.01 and row['ROAS'] >= 1:
            return ("Cost-Effective Scenario", "CPA < 300, CTR > 0.01, ROAS >= 1")
        
        # Reassess Scenario
        elif row['ROAS'] < 1 or row['CPA'] > 700 or row['CTR'] < 0.01:
            return ("Reassess Scenario", "ROAS < 1, CPA > 700, CTR < 0.01")
        
        # Default case
        else:
            return ("Campaign is too new to assess the performance", "N/A")

    ## Apply the rules to each row in the DataFrame
    results, reasoning = zip(*campaign_df.apply(apply_rules, axis=1))
    campaign_df['scenario'] = results
    campaign_df['reasoning'] = reasoning


    return campaign_df.to_dict()


@app_k.post("/campaign_budget_reallocation_based_on_performance")
def campaign_budget_reallocation_based_on_performance():
    sql_query = """SELECT 
    campaign_id, 
    SUM(metrics_clicks) as clicks,
    SUM(metrics_conversions) as conversions, 
    SUM(metrics_cost_micros)/100000000 as cost,
    SUM(metrics_impressions) as impressions,
    sum(metrics_conversions_value)/10 as conversion_value   
FROM 
    `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats_2222881946` 
WHERE 
    segments_date > DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
GROUP BY 
    campaign_id;"""
    # Run the query
    query_job = client.query(sql_query)

    # Convert the result to a DataFrame
    campaign_df = query_job.to_dataframe()

    # Calculate CPA, CTR, and ROAS
    campaign_df['CPA'] = campaign_df['cost'] / campaign_df['conversions']
    campaign_df['CTR'] = campaign_df['clicks'] / campaign_df['impressions']    
    campaign_df['ROAS'] = campaign_df['conversion_value'] / campaign_df['cost']

    
    

    # Handle potential division by zero
    campaign_df['CPA'] = campaign_df['CPA'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['CTR'] = campaign_df['CTR'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['ROAS'] = campaign_df['ROAS'].replace([np.inf, -np.inf], np.nan).fillna(0)

    campaign_df['CPA'] = campaign_df['CPA'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['CTR'] = campaign_df['CTR'].replace([np.inf, -np.inf], np.nan).fillna(0)
    campaign_df['ROAS'] = campaign_df['ROAS'].replace([np.inf, -np.inf], np.nan).fillna(0)

    def apply_rules(row):
        # High-Performance Scenario
        if row['ROAS'] > 1.5 and row['CPA'] < 300 and row['CTR'] > 0.025:
            return ("High-Performance Scenario", "ROAS > 1.5, CPA < 300, CTR > 0.025")
        
        # Moderate-Performance Scenario
        elif 1 <= row['ROAS'] <= 1.5 and 300 <= row['CPA'] <= 700 and 0.01 <= row['CTR'] <= 0.025:
            return ("Moderate-Performance Scenario", "1 <= ROAS <= 1.5, 300 <= CPA <= 700, 0.01 <= CTR <= 0.025")
        
        # Optimization Opportunity Scenario
        elif row['CTR'] > 0.025 and row['CPA'] < 300 and row['ROAS'] >= 1:
            return ("Optimization Opportunity Scenario", "CTR > 0.025, CPA < 300, ROAS >= 1")
        
        # Cost-Effective Scenario
        elif row['CPA'] < 300 and row['CTR'] > 0.01 and row['ROAS'] >= 1:
            return ("Cost-Effective Scenario", "CPA < 300, CTR > 0.01, ROAS >= 1")
        
        # Reassess Scenario
        elif row['ROAS'] < 1 or row['CPA'] > 700 or row['CTR'] < 0.01:
            return ("Reassess Scenario", "ROAS < 1, CPA > 700, CTR < 0.01")
        
        # Default case
        else:
            return ("Campaign is too new to evaluate and reallocate", "N/A")

    # Apply the rules to each row in the DataFrame
    results, reasoning = zip(*campaign_df.apply(apply_rules, axis=1))
    campaign_df['scenario'] = results
    campaign_df['reasoning'] = reasoning

    # Apply the rules to each row in the DataFrame
    

    # Create a new DataFrame without the "Reassess" scenarios
    new_campaign_df = campaign_df[~campaign_df['scenario'].str.contains("Reassess")]

    # Calculate the total cost of the "Reassess" campaigns
    reassess_cost = campaign_df.loc[campaign_df['scenario'].str.contains("Reassess"), 'cost'].sum()

    # Find the number of other campaigns
    other_campaigns_count = campaign_df.loc[~campaign_df['scenario'].str.contains("Reassess")].shape[0]

    # Calculate the additional budget per campaign
    additional_budget = reassess_cost / other_campaigns_count if other_campaigns_count > 0 else 0

    # Add the additional budget to the 'new_budget' of each campaign in new_campaign_df
    new_campaign_df['new_budget'] = new_campaign_df['cost'] + additional_budget

    # Calculate the new revenue
    new_campaign_df['new_revenue'] = new_campaign_df['ROAS'] * new_campaign_df['new_budget']

    return new_campaign_df.to_dict()

#print(campaign_budget_reallocation_based_on_performance())



# Assuming you already have a DataFrame named ad_copy_df
# Assumed thresholds for each metric
CTR_good_threshold = 2.0
CTR_moderate_threshold = 1.0
conversion_rate_good_threshold = 5.0
conversion_rate_moderate_threshold = 3.0
cost_per_conversion_good_threshold = 300.0
cost_per_conversion_moderate_threshold = 500.0  # Updated threshold for lower values
ROAS_good_threshold = 1.5
ROAS_moderate_threshold = 0.7
CPC_good_threshold = 0.8  # Updated threshold for lower values
CPC_moderate_threshold = 1.2
interaction_rate_good_threshold = 4.0
interaction_rate_moderate_threshold = 2.0
view_through_conversion_rate_good_threshold = 1.0
view_through_conversion_rate_moderate_threshold = 0.5

# Assumed weights for each metric
weights = {
    'CTR': 1,
    'Conversion Rate': 1,
    'Cost per Conversion': 1,
    'ROAS': 1,
    'CPC': 1
}

# Function to categorize performance into buckets
def categorize_performance(value, threshold_good, threshold_moderate, lower_is_better=False):
    if lower_is_better:
        if value <= threshold_good:
            return 'Good'
        elif value <= threshold_moderate:
            return 'Moderate'
        else:
            return 'Poor'
    else:
        if value >= threshold_good:
            return 'Good'
        elif value >= threshold_moderate:
            return 'Moderate'
        else:
            return 'Poor'

# Function to calculate individual performance labels
def calculate_individual_labels(df):
    df['CTR_Label'] = df['CTR'].apply(lambda x: categorize_performance(x, CTR_good_threshold, CTR_moderate_threshold))
    df['Conversion_Rate_Label'] = df['conversion_rate'].apply(lambda x: categorize_performance(x, conversion_rate_good_threshold, conversion_rate_moderate_threshold))
    df['Cost_per_Conversion_Label'] = df['cost_per_conversion'].apply(lambda x: categorize_performance(x, cost_per_conversion_good_threshold, cost_per_conversion_moderate_threshold, lower_is_better=True))
    df['ROAS_Label'] = df['ROAS'].apply(lambda x: categorize_performance(x, ROAS_good_threshold, ROAS_moderate_threshold))
    df['CPC_Label'] = df['CPC'].apply(lambda x: categorize_performance(x, CPC_good_threshold, CPC_moderate_threshold, lower_is_better=True))
    return df

# Function to categorize overall performance into buckets
def categorize_overall_performance(score, threshold_good, threshold_moderate):
    if score >= threshold_good:
        return 'Good'
    elif score >= threshold_moderate:
        return 'Moderate'
    else:
        return 'Poor'

# Function to fetch ad copy performance data
@app_k.post("/adcopy_performance_for_search")
def adcopy_performance():
    # Assumed BigQuery client initialization
    # Please replace this with your actual client initialization
    # client = bigquery.Client()

    # SQL Query
    sql_query_4 = """
        SELECT 
            ad_group_ad_ad_id,
            campaign_id,
            CASE WHEN SUM(metrics_impressions) > 0 THEN (SUM(metrics_clicks)*10 / SUM(metrics_impressions)) ELSE 0 END as CTR,
            CASE WHEN SUM(metrics_clicks) > 0 THEN (SUM(metrics_conversions)*100 / SUM(metrics_clicks)) ELSE 0 END as conversion_rate,
            CASE WHEN SUM(metrics_conversions) > 0 THEN (SUM(metrics_cost_micros)/1000000) / SUM(metrics_conversions) ELSE 0 END as cost_per_conversion,
            CASE WHEN SUM(metrics_cost_micros) > 0 THEN SUM(metrics_conversions_value) / (SUM(metrics_cost_micros) / 1000000) ELSE 0 END as ROAS,
            CASE WHEN SUM(metrics_clicks) > 0 THEN (SUM(metrics_cost_micros)/1000000) / SUM(metrics_clicks) ELSE 0 END as CPC,
            CASE WHEN SUM(metrics_impressions) > 0 THEN (SUM(metrics_interactions) / SUM(metrics_impressions)) ELSE 0 END as interaction_rate,
            CASE WHEN SUM(metrics_impressions) > 0 THEN (SUM(metrics_view_through_conversions) / SUM(metrics_impressions))  ELSE 0 END as view_through_conversion_rate
        FROM `ga4-bq-connector.DemoData.p_ads_AdBasicStats_2222881946`
        WHERE segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 21 DAY)
        AND segments_ad_network_type = 'SEARCH'
        GROUP BY ad_group_ad_ad_id, campaign_id
        """

    # Run the query and convert the result to a DataFrame
    query_job = client.query(sql_query_4)
    ad_copy_df = query_job.to_dataframe()
    
    # Calculate overall scores
    ad_copy_df['Overall_Score_out_of_5'] = (
            weights['CTR'] * (ad_copy_df['CTR'] >= CTR_good_threshold) +
            weights['Conversion Rate'] * (ad_copy_df['conversion_rate'] >= conversion_rate_good_threshold) +
            weights['Cost per Conversion'] * (ad_copy_df['cost_per_conversion'] <= cost_per_conversion_good_threshold) +
            weights['ROAS'] * (ad_copy_df['ROAS'] >= ROAS_good_threshold) +
            weights['CPC'] * (ad_copy_df['CPC'] <= CPC_good_threshold) 
            )
    
    # Add overall performance label based on scores
    
    ad_copy_df['Overall_Performance_Label'] = ad_copy_df['Overall_Score_out_of_5'].apply(lambda x: categorize_overall_performance(x, 4, 3)) 
    
    #ad_copy_df.to_csv('ad_copy_df3.csv', index=False)
    # Calculate individual performance labels
    ad_copy_df = calculate_individual_labels(ad_copy_df)

    #ad_copy_df.to_csv('ad_copy_df.csv', index=False)
    return ad_copy_df.to_dict()

#pd.DataFrame(campaign_budget_reallocation_based_on_performance()).to_csv('campaign_budget_reallocation_based_on_performance.csv', index=False) 

#pd.DataFrame(identify_negative_keywords()).to_csv('campaign_evaluation_based_on_budget.csv', index=False) 

@app_k.post("/budget_vs_spend_for_last_28_days")
def budget_vs_spend():
    # Define the SQL query
    sql_query = """
    SELECT 
        stats.campaign_budget_id,
        stats.campaign_id,
        stats.customer_id,
        stats.campaign_budget_recommended_budget_estimated_change_weekly_clicks,
        stats.campaign_budget_recommended_budget_estimated_change_weekly_cost_micros,
        stats.campaign_budget_recommended_budget_estimated_change_weekly_interactions,
        stats.campaign_budget_recommended_budget_estimated_change_weekly_views,
        stats.campaign_name,
        stats.campaign_status,
        stats.metrics_all_conversions,
        stats.metrics_all_conversions_from_interactions_rate,
        stats.metrics_all_conversions_value,
        stats.metrics_average_cost,
        stats.metrics_average_cpc,
        stats.metrics_average_cpe,
        stats.metrics_average_cpm,
        stats.metrics_average_cpv,
        stats.metrics_clicks,
        stats.metrics_conversions,
        stats.metrics_conversions_from_interactions_rate,
        stats.metrics_conversions_value,
        stats.metrics_cost_micros,
        stats.metrics_cost_per_all_conversions,
        stats.metrics_cost_per_conversion,
        stats.metrics_cross_device_conversions,
        stats.metrics_ctr,
        stats.metrics_engagement_rate,
        stats.metrics_engagements,
        stats.metrics_impressions,
        stats.metrics_interaction_event_types,
        stats.metrics_interaction_rate,
        stats.metrics_interactions,
        stats.metrics_value_per_all_conversions,
        stats.metrics_value_per_conversion,
        stats.metrics_video_view_rate,
        stats.metrics_video_views,
        stats.metrics_view_through_conversions,
        stats.segments_date,
        budget.campaign_budget_amount_micros,
        budget.campaign_budget_delivery_method,
        budget.campaign_budget_explicitly_shared,
        budget.campaign_budget_has_recommended_budget,
        budget.campaign_budget_name,
        budget.campaign_budget_period,
        budget.campaign_budget_recommended_budget_amount_micros,
        budget.campaign_budget_reference_count,
        budget.campaign_budget_status,
        budget.campaign_budget_total_amount_micros,
        budget.customer_descriptive_name
    FROM 
        `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_BudgetStats_2222881946` AS stats
    JOIN 
        `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_Budget_2222881946` AS budget
    ON 
        stats.campaign_budget_id = budget.campaign_budget_id AND 
        stats.customer_id = budget.customer_id
    WHERE 
        segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY) AND
        campaign_budget_period = 'DAILY'
    """

    # Run the query and load the result into a DataFrame
    df = client.query(sql_query).to_dataframe()

    # Group by campaign_id, campaign_budget_id, and segments_date, and calculate the sum of metrics_cost_micros and campaign_budget_amount_micros
    df_grouped = df.groupby(['campaign_id', 'campaign_budget_id', 'segments_date']).agg({
        'metrics_cost_micros': 'sum',
        'campaign_budget_amount_micros': 'sum'
    }).reset_index()

    # Rename the columns for clarity
    df_grouped.rename(columns={
        'metrics_cost_micros': 'total_spend',
        'campaign_budget_amount_micros': 'total_budget'
    }, inplace=True)

    # Divide metrics_cost_micros and campaign_budget_amount_micros by 1e6
    df_grouped['total_spend'] = df_grouped['total_spend'] / 1e6
    df_grouped['total_budget'] = df_grouped['total_budget'] / 1e6

    # Group by campaign_id and calculate the sum of total_spend and total_budget
    df_campaignwise = df_grouped.groupby('campaign_id').agg({
        'total_spend': 'sum',
        'total_budget': 'sum'
    }).reset_index()

    return df_grouped.to_dict()

print(adcopy_performance())
