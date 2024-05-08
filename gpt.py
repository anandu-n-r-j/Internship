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


app = FastAPI()
client = bigquery.Client.from_service_account_json(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
    project='ga4-bq-connector'
)

@app.post("/increase_bidrate_keywords")
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



@app.post("/keywords_and_metrics")
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


@app.post("/identify_negative_keywords")
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


@app.post("/optimize_campaigns_on_roas_for_last_3_weeks")
def optimize_campaigns_on_roas():
    # Define the SQL query
    sql_query = """
        SELECT campaign_id,
        campaign_name,
        SUM(metrics_conversions_value) AS revenue,
        SUM(metrics_cost_micros)/1000000 AS cost 
FROM `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats`
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

@app.post("/get_campaigns_based_on_roas")
def get_campaigns_based_on_roas():
    # Define the SQL query
    sql_query = """
    SELECT
        campaign_id,
        SUM(metrics_conversions_value) AS revenue,
        SUM(metrics_cost_micros)/1000000 AS cost
    FROM `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats`
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

@app.post("/campaign_evaluation_based_on_budget")
def campaign_evaluation_based_on_budget():
    sql_query = """SELECT 
    campaign_id, 
    SUM(metrics_clicks) as clicks,
    SUM(metrics_conversions) as conversions, 
    SUM(metrics_cost_micros)/100000000 as cost,
    SUM(metrics_impressions) as impressions,
    sum(metrics_conversions_value)/10 as conversion_value   
FROM 
    `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats` 
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


@app.post("/campaign_budget_reallocation_based_on_performance")
def campaign_budget_reallocation_based_on_performance():
    sql_query = """SELECT 
    campaign_id, 
    SUM(metrics_clicks) as clicks,
    SUM(metrics_conversions) as conversions, 
    SUM(metrics_cost_micros)/100000000 as cost,
    SUM(metrics_impressions) as impressions,
    sum(metrics_conversions_value)/10 as conversion_value   
FROM 
    `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats` 
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
cost_per_conversion_good_threshold = 30.0
cost_per_conversion_moderate_threshold = 50.0  # Updated threshold for lower values
ROAS_good_threshold = 4.0
ROAS_moderate_threshold = 3.0
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
    'CPC': 1,
    'Interaction Rate': 1,
    'View-Through Conversion Rate': 1
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
    df['Interaction_Rate_Label'] = df['interaction_rate'].apply(lambda x: categorize_performance(x, interaction_rate_good_threshold, interaction_rate_moderate_threshold))
    df['View_Through_Conversion_Rate_Label'] = df['view_through_conversion_rate'].apply(lambda x: categorize_performance(x, view_through_conversion_rate_good_threshold, view_through_conversion_rate_moderate_threshold))
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
@app.post("/adcopy_performance_for_search")
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
        FROM `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_AdBasicStats_2222881946`
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
            weights['CPC'] * (ad_copy_df['CPC'] <= CPC_good_threshold) +
            weights['Interaction Rate'] * (ad_copy_df['interaction_rate'] >= interaction_rate_good_threshold) +
            weights['View-Through Conversion Rate'] * (ad_copy_df['view_through_conversion_rate'] >= view_through_conversion_rate_good_threshold)
        )
    
    # Add overall performance label based on scores
    
    ad_copy_df['Overall_Performance_Label'] = ad_copy_df['Overall_Score_out_of_5'].apply(lambda x: categorize_overall_performance(x, 4, 3)) 
    
    #ad_copy_df.to_csv('ad_copy_df3.csv', index=False)
    # Calculate individual performance labels
    ad_copy_df = calculate_individual_labels(ad_copy_df)

    
    return ad_copy_df.to_dict()

#pd.DataFrame(campaign_budget_reallocation_based_on_performance()).to_csv('campaign_budget_reallocation_based_on_performance.csv', index=False) 

#pd.DataFrame(identify_negative_keywords()).to_csv('campaign_evaluation_based_on_budget.csv', index=False) 

@app.post("/budget_vs_spend_analysis_1_week")


def budget_vs_spend_analysis_1_week():
        # Provided table prefix
        table_prefix = "ga4-bq-connector.TDKGoogleAdsDataset"

        # SQL query
        query_daily_1_week = f"""
                WITH relevant_data AS (
    SELECT
        b.campaign_budget_id,
        b.customer_id, 
        ROUND(b.campaign_budget_amount_micros/1e6) AS daily_budget,
        ROUND(bs.metrics_cost_micros/1e6) AS cost,
        bs.segments_date
    FROM
        `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_Budget_2222881946` b
    JOIN
        `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_BudgetStats_2222881946` bs
    ON
        b.campaign_budget_id = bs.campaign_budget_id
        AND b.customer_id = bs.customer_id
    WHERE
        b.campaign_budget_period = 'DAILY'
        AND bs.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 WEEK)
)
SELECT
    segments_date AS date,
    SUM(ROUND(daily_budget)) AS total_daily_budget,
    SUM(ROUND(cost)) AS total_daily_cost
FROM
    relevant_data
GROUP BY
    date
ORDER BY
    date;
        """

        # Execute query
        df_daily_1_week = client.query(query_daily_1_week).to_dataframe()

        # Convert to dictionary
        result_daily_1_week = df_daily_1_week.to_dict()

        return {"budget_vs_spend_1_week": result_daily_1_week}

@app.post("/budget_vs_spend_analysis_6_months")

def budget_vs_spend_analysis_6_months():
        # Provided table prefix
        table_prefix = "ga4-bq-connector.TDKGoogleAdsDataset"

        # SQL query
        query_monthly_6_months = f"""
                WITH relevant_data AS (
    SELECT
        b.campaign_budget_id,
        b.customer_id,  
        b.campaign_budget_amount_micros/1e6 AS daily_budget,
        bs.metrics_cost_micros/1e6 AS cost,
        DATE_TRUNC(bs.segments_date, MONTH) AS month_start
    FROM
        `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_Budget_2222881946` b
    JOIN
        `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_BudgetStats_2222881946` bs
    ON
        b.campaign_budget_id = bs.campaign_budget_id
        AND b.customer_id = bs.customer_id
    WHERE
        b.campaign_budget_period = 'DAILY'
        AND bs.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
)
SELECT
    FORMAT_DATE('%B %Y', month_start) AS month,
    SUM(ROUND(daily_budget)) AS total_monthly_budget,
    SUM(ROUND(cost)) AS total_monthly_cost
FROM
    relevant_data
GROUP BY
    month_start
ORDER BY
    month_start; -- Order by the month_start column
        """

        # Execute query
        df_monthly_6_months = client.query(query_monthly_6_months).to_dataframe()

        # Convert to dictionary
        result_monthly_6_months = df_monthly_6_months.to_dict()

        return {"budget_vs_spend_6_months": result_monthly_6_months}


@app.post("/segment_slot_and_device_combination")
def get_segment_slot_and_device_combination():
    query = """
        SELECT 
        segments_slot,
        segments_device, 
        COALESCE(sum(metrics_clicks)/NULLIF(sum(metrics_impressions), 0), -1.0) as click_through_rate,
        COALESCE(sum(metrics_conversions)/NULLIF(sum(metrics_clicks), 0), -1.0) as conversion_rate,
        sum(metrics_conversions_value) as revenue_generated,
        sum(metrics_cost_micros)/1e6 as amount_spent,
        COALESCE(sum(metrics_conversions_value)/NULLIF(sum(metrics_cost_micros)/1e6, 0), -1.0) as RoAS
    FROM 
        ga4-bq-connector.DemoData.p_ads_AdBasicStats
    GROUP BY 
        segments_slot, 
        segments_device
    """
    result_df = client.query(query).to_dataframe()
    result_dict = result_df.to_dict()

    # Replace -1 with '-' in the dictionary
    for key in result_dict.keys():
        result_dict[key] = {k: ('-' if v == -1 else v) for k, v in result_dict[key].items()}

    return {"data": result_dict}


@app.post("/analyse_hour_of_day_for_campaign")
def analysis_Hourly_campaign():
    query = """
    SELECT 
        ROUND(SUM(hcs.metrics_clicks) / NULLIF(SUM(hcs.metrics_impressions), 0), 2) as click_through_rate,
        ROUND(SUM(hcs.metrics_cost_micros) / 1e6, 2) as spend,
        ROUND(SUM(hcs.metrics_conversions) / NULLIF(SUM(hcs.metrics_clicks), 0), 2) as conversions,
        SUM(hcs.metrics_impressions) as total_impressions,
        SUM(hcs.metrics_clicks) as total_clicks,
        SUM(hcs.metrics_conversions) as total_conversions,
        hcs.segments_device,
        hcs.segments_hour
    FROM 
        ga4-bq-connector.TDKGoogleAdsDataset.p_ads_HourlyCampaignStats_2222881946 as hcs
    JOIN 
        ga4-bq-connector.DemoData.campaign_mapping as cm
    ON 
        CAST(hcs.campaign_id AS STRING) = cm.campaign_id
    WHERE 
        hcs.segments_date >= DATE_SUB(CURRENT_DATE, INTERVAL 3 WEEK)
    GROUP BY 
        hcs.segments_device,
        hcs.segments_hour
    """
    query_job = client.query(query)

    # Get the results
    results = query_job.result()

    # Convert the results to a pandas DataFrame
    df = results.to_dataframe()

    # Replace NaN and Infinity values with appropriate values
    df.replace([np.nan, np.inf, -np.inf], 0, inplace=True)

    # Convert the DataFrame to a dictionary
    query_results_dict = df.to_dict()

    return query_results_dict

# Run the function to execute the query and get the results as a dictionary
query_results_dict = analysis_Hourly_campaign()
#print(query_results_dict)

@app.post("/get_campaign_conversion_stats_for_4_and_6_weeks")
def get_campaign_conversion_stats():

    # Query for last 4 weeks
    query_last_4_weeks = """
    WITH last_4_weeks AS (
      SELECT
        CONCAT(EXTRACT(YEAR FROM segments_date), '-', EXTRACT(WEEK FROM segments_date)) AS week_year,
        campaign_id,
        SUM(metrics_clicks) AS total_clicks,
        SUM(metrics_conversions) AS total_conversions,
        SUM(metrics_conversions) / SUM(metrics_clicks) AS conversion_rate
      FROM
        `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats`
      WHERE
        segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
      GROUP BY
        campaign_id, week_year
    )
    SELECT
      'Last 4 Weeks' AS period,
      week_year,
      MAX(CASE WHEN conversion_rank_desc = 1 THEN campaign_id END) AS highest_conversion_campaign,
      MAX(CASE WHEN conversion_rank_desc = 1 THEN highest_conversion_rate END) AS highest_conversion_rate,
      MAX(CASE WHEN conversion_rank_asc = 1 THEN campaign_id END) AS lowest_conversion_campaign,
      MAX(CASE WHEN conversion_rank_asc = 1 THEN lowest_conversion_rate END) AS lowest_conversion_rate
    FROM (
      SELECT
        ROW_NUMBER() OVER (PARTITION BY week_year ORDER BY conversion_rate DESC) AS conversion_rank_desc,
        ROW_NUMBER() OVER (PARTITION BY week_year ORDER BY conversion_rate ASC) AS conversion_rank_asc,
        week_year,
        campaign_id,
        total_conversions / total_clicks AS highest_conversion_rate,
        total_conversions / total_clicks AS lowest_conversion_rate
      FROM
        last_4_weeks
    ) ranks
    GROUP BY
      week_year
    ORDER BY
      week_year, period;
    """

    # Query for last 6 weeks
    query_last_6_weeks = """
    WITH last_6_weeks AS (
    SELECT
        CONCAT(EXTRACT(YEAR FROM segments_date), '-', EXTRACT(WEEK FROM segments_date)) AS week_year,
        campaign_id,
        SUM(metrics_clicks) AS total_clicks,
        SUM(metrics_conversions) AS total_conversions,
        SUM(metrics_conversions) / SUM(metrics_clicks) AS conversion_rate
    FROM
        `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats`
    WHERE
        segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 WEEK)
    GROUP BY
        campaign_id, week_year
)
SELECT
    'Last 6 Weeks' AS period,
    week_year,
    MAX(CASE WHEN conversion_rank_desc = 1 THEN campaign_id END) AS highest_conversion_campaign,
    MAX(CASE WHEN conversion_rank_desc = 1 THEN highest_conversion_rate END) AS highest_conversion_rate,
    MAX(CASE WHEN conversion_rank_asc = 1 THEN campaign_id END) AS lowest_conversion_campaign,
    MAX(CASE WHEN conversion_rank_asc = 1 THEN lowest_conversion_rate END) AS lowest_conversion_rate
FROM (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY week_year ORDER BY conversion_rate DESC) AS conversion_rank_desc,
        ROW_NUMBER() OVER (PARTITION BY week_year ORDER BY conversion_rate ASC) AS conversion_rank_asc,
        week_year,
        campaign_id,
        total_clicks, -- Corrected from total
        total_conversions / total_clicks AS highest_conversion_rate,
        total_conversions / total_clicks AS lowest_conversion_rate
    FROM
        last_6_weeks
) ranks
GROUP BY
    week_year
ORDER BY
    week_year, period;

    """

    # Execute the queries
    query_job_4_weeks = client.query(query_last_4_weeks)
    query_job_6_weeks = client.query(query_last_6_weeks)

    # Wait for the job to complete
    results_4_weeks = query_job_4_weeks.result()
    results_6_weeks = query_job_6_weeks.result()

    # Convert results to Pandas DataFrames
    df_4_weeks = results_4_weeks.to_dataframe()
    df_6_weeks = results_6_weeks.to_dataframe()
   
    # Convert DataFrames to dictionaries
    dict_last_4_weeks = df_4_weeks.to_dict(orient='records')
    dict_last_6_weeks = df_6_weeks.to_dict(orient='records')

    return dict_last_4_weeks, dict_last_6_weeks

# Get data as dictionaries
#get_campaign_conversion_stats()


@app.post("/performance_ads_network")
def performance_ads_network():
    # SQL query
    query = """
    SELECT
      segments_ad_network_type AS Ad_Network_Type,
      SUM(metrics_clicks) AS Total_Clicks,
      SUM(metrics_impressions) AS Total_Impressions,
      IFNULL(SUM(metrics_clicks) / NULLIF(SUM(metrics_impressions), 0), 0) AS CTR,
      SUM(metrics_cost_micros) / 1000000 AS Total_Cost,
      IFNULL(SUM(metrics_conversions), 0) AS Total_Conversions,
      IFNULL(SUM(metrics_conversions_value), 0) AS Total_Conversion_Value
    FROM
      `ga4-bq-connector.DemoData.p_ads_CampaignBasicStats`
    WHERE
      segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK) -- Data for the last 4 weeks
    GROUP BY
      Ad_Network_Type
    ORDER BY
     Ad_Network_Type ASC;
    """

    # Execute the query and convert results to DataFrame
    df = client.query(query).to_dataframe()
    

    # Convert DataFrame to dictionary
    result_dict = df.to_dict(orient='records')

    return result_dict

# Example usage:
#result_dict = performance_ads_network()
#print(result_dict)

def get_keyword_conversions():
   
    query = """
    SELECT
    CONCAT(EXTRACT(YEAR FROM kcs.segments_date), '-', EXTRACT(WEEK FROM kcs.segments_date)) AS week_year,
    k.ad_group_criterion_keyword_text AS keyword,
    SUM(CASE WHEN kcs.metrics_conversions = 0 THEN 0 ELSE 1 END) AS total_conversions

FROM
    `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_KeywordConversionStats_2222881946` AS kcs
JOIN
    `ga4-bq-connector.DemoData.p_ads_Keyword` AS k
ON
    kcs.campaign_id = k.campaign_id
WHERE
    kcs.segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 WEEK)
GROUP BY
    week_year,
    keyword
ORDER BY
    week_year DESC,
    total_conversions DESC;

  
    """


    query_job = client.query(query)
    results = query_job.result()


    # Save results into a DataFrame
    df = pd.DataFrame(results)
    

    
    # Convert DataFrame to dictionary
    result_dict = df.to_dict()


    return result_dict


keyword_conversions = get_keyword_conversions()
print(keyword_conversions)


