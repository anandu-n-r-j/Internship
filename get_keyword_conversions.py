def get_keyword_conversions():

   

    query = """

    SELECT

      CONCAT(EXTRACT(YEAR FROM segments_date), '-', EXTRACT(WEEK FROM segments_date)) AS week_year,

      segments_conversion_action_name AS keyword,

      SUM(metrics_conversions) AS total_conversions

    FROM

      `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_KeywordConversionStats_2222881946`

    WHERE

      segments_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 WEEK)

     GROUP BY

      week_year,

      segments_conversion_action_name

    ORDER BY

      week_year ASC,

      total_conversions DESC;

    """



    query_job = client.query(query)

    results = query_job.result()



    # Save results into a DataFrame

    df = pd.DataFrame(results)

    print(df)



    # Convert DataFrame to dictionary

    result_dict = df.to_dict()



    return result_dict



keyword_conversions = get_keyword_conversions()

print(keyword_conversions)