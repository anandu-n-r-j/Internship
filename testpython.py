from google.cloud import bigquery
import pandas as pd

def identify_and_export_negative_keywords(ctr_threshold=0.05, conversion_rate_threshold=2.0, output_csv_path='output_keywords.csv'):
    client = bigquery.Client()

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

    # Create a new column 'Potential_Negative_Keywords' based on undesired terms, CTR, and Conversion_rate
    df['Potential_Negative_Keywords'] = df.apply(lambda row: 'Undesired Term'
                                                  if any(term in row['KeywordText'].lower() for term in undesired_terms)
                                                  else 'Potential Negative Keyword: High CTR, Low Conversions'
                                                  if row['CTR'] > ctr_threshold and row['Conversion_rate'] < conversion_rate_threshold
                                                  else 'Performing Well', axis=1)

    # Export the updated DataFrame to a CSV file
    df.to_csv(output_csv_path, index=False)

    print(f"CSV file saved at: {output_csv_path}")

# Example usage:
identify_and_export_negative_keywords()
