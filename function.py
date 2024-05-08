def analyze_campaign_device_performance():
    thresholds = {
        'cross_device_conversions_threshold': 100,
        'engagement_rate_threshold': 0.05,
        'phone_calls_threshold': 50,
        'phone_through_rate_threshold': 2,
        'impression_share_threshold': 0.8,
        'top_impression_percentage_threshold': 0.8,
        'new_visitors_percentage_threshold': 0.2,
        'time_on_site_threshold': 60,
        'bounce_rate_threshold': 0.4
    }

    # Define SQL query
    sql_query = f"""
    SELECT
        campaign_id,
        customer_id,
        metrics_cross_device_conversions,
        metrics_engagement_rate,
        metrics_phone_calls,
        metrics_phone_through_rate,
        metrics_search_impression_share,
        metrics_top_impression_percentage,
        metrics_percent_new_visitors,
        metrics_average_time_on_site,
        metrics_bounce_rate
    FROM
         `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_CampaignCrossDeviceStats_2222881946`
    WHERE
        _PARTITIONTIME >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 28 DAY)
    """

    # Execute the SQL query
    query_job = client.query(sql_query)

    # Process the results and identify interpretations
    results = []
    for row in query_job:
        campaign_id = row['campaign_id']
        customer_id = row['customer_id']
        interpretations = {}
        # Check each metric against its threshold and provide interpretation
        interpretations['campaign_id'] = campaign_id
        interpretations['customer_id'] = customer_id
        interpretations['cross_device_conversions'] = 'Lower cross-device conversions. It may indicate a need for improvements in cross-device targeting or user experiences.' if row['metrics_cross_device_conversions'] < thresholds['cross_device_conversions_threshold'] else 'Higher cross-device conversions may suggest effective cross-device targeting strategies or user-friendly experiences across devices.'
        interpretations['engagement_rate'] = 'Higher engagement rates or engagement counts may indicate effective ad content or targeting.' if row['metrics_engagement_rate'] > thresholds['engagement_rate_threshold'] else 'Lower engagement rates. It may suggest that the ad content or targeting needs improvement to engage users more effectively.'
        interpretations['phone_calls'] = 'Lower phone calls or through rates. It may indicate that the mobile ad campaigns or landing pages are not sufficiently optimized to drive conversions.' if row['metrics_phone_calls'] < thresholds['phone_calls_threshold'] or row['metrics_phone_through_rate'] < thresholds['phone_through_rate_threshold'] else 'A high number of phone calls or through rates may suggest effective mobile ad campaigns or optimized mobile landing pages.'
        interpretations['impression_share'] = 'Lower impression share on certain devices may indicate opportunities for optimization.' if row['metrics_search_impression_share'] < thresholds['impression_share_threshold'] or row['metrics_top_impression_percentage'] < thresholds['top_impression_percentage_threshold'] else 'Higher impression share. It may suggest that the ad visibility on different devices is good and there may be less room for optimization.'
        interpretations['new_visitors_percentage'] = 'Lower percentages. It may indicate that the ad visibility, particularly on mobile devices, needs improvement.' if row['metrics_percent_new_visitors'] < thresholds['new_visitors_percentage_threshold'] else 'Higher percentages may indicate better ad visibility, especially on mobile devices with limited screen space.'
        interpretations['time_on_site'] = 'Lower time on site, possibly indicating less engagement or relevance of the content.' if row['metrics_average_time_on_site'] < thresholds['time_on_site_threshold'] else 'Higher time on site. It may suggest that users find the content engaging and relevant, leading to longer durations on the site.'
        interpretations['bounce_rate'] = 'Lower bounce rates. It indicates that users are finding the landing page or ad content relevant and engaging.' if row['metrics_bounce_rate'] < thresholds['bounce_rate_threshold'] else 'Higher bounce rates may suggest that the landing page or ad content needs improvement.'

        # Add interpretations to the results list
        results.append(interpretations)

    return results


results = analyze_campaign_device_performance()
print(results)





from google.cloud import bigquery
from datetime import datetime, timedelta

def seasonal_roas_interpretations():
    # Initialize BigQuery client
    #client = bigquery.Client()

    # Define the end date as today
    end_date = datetime.today()

    # Define the start date as 3 years before today
    start_date = datetime.today() - timedelta(days=3*365)

    # Define the SQL query with parameters
    query = """
        SELECT EXTRACT(MONTH FROM campaign_start_date) AS month,
               EXTRACT(YEAR FROM campaign_start_date) AS year,
               campaign_id,
               AVG(campaign_maximize_conversion_value_target_roas) AS mean_roas,
               STDDEV(campaign_maximize_conversion_value_target_roas) AS std_roas
        FROM `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_Campaign_2222881946`
        WHERE campaign_start_date BETWEEN DATE('{}') AND DATE('{}')
        GROUP BY month, year, campaign_id
        ORDER BY year, month, campaign_id
    """.format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

    # Execute the query
    query_job = client.query(query)

    # Convert the results to a DataFrame
    results = query_job.result().to_dataframe()

    # Create an empty list to store the result dictionaries
    result_list = []

    # Iterate over each row in the results DataFrame
    for index, row in results.iterrows():
        # Create a dictionary to store the summary statistics and interpretations
        result_dict = {
            'month': int(row['month']),
            'year': int(row['year']),
            'campaign_id': int(row['campaign_id']),
            'mean_roas': row['mean_roas'],
            'std_roas': row['std_roas']
        }

        # Interpretations based on the values
        interpretation = []
        if row['mean_roas'] > 0.8:
            interpretation.append("The mean ROAS is higher than 0.8, indicating good performance.")
        else:
            interpretation.append("The mean ROAS is lower than 0.8, indicating potential room for improvement.")
        
        if row['std_roas'] < 0.05:
            interpretation.append("The standard deviation of ROAS is low, suggesting consistent performance.")
        else:
            interpretation.append("The standard deviation of ROAS is high, suggesting variability in performance.")
        
        result_dict['interpretation'] = interpretation

        # Append the result dictionary to the list
        result_list.append(result_dict)

    return result_list
