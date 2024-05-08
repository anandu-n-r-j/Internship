import pandas as pd
import numpy as np

sql_query = """
SELECT *
FROM `ga4-bq-connector.DemoData.audience_targeting_refining_demo_data1`
"""

# Execute the query
query_job = client.query(sql_query)

# Convert the results to a pandas DataFrame
df = query_job.to_dataframe()
# Separation line between data creation and analysis


# Recommendations for Use Case 1: Identifying High-Performing Demographics
high_performing_demo = df.groupby(['Age', 'Gender'], as_index=False).agg({
    'Clicks': 'mean',
    'Conversions': 'mean',
    'CTR': 'mean',
    'Conversion Rate': 'mean'
}).sort_values('Conversion Rate', ascending=False).head(1)

demo_improvement_rate = 10  # Assume a 10% improvement in conversion rate

recommendation_demo_1 = f"Target the {high_performing_demo['Age'].iloc[0]}-{high_performing_demo['Gender'].iloc[0]} age group for higher conversions. This adjustment could potentially result in a {demo_improvement_rate}% increase in conversion rate."

low_performing_demo = df.groupby(['Age', 'Gender'], as_index=False).agg({
    'Clicks': 'mean',
    'Conversions': 'mean',
    'CTR': 'mean',
    'Conversion Rate': 'mean'
}).sort_values('Conversion Rate').head(1)

recommendation_demo_2 = f"Avoid targeting the {low_performing_demo['Age'].iloc[0]}-{low_performing_demo['Gender'].iloc[0]} age group due to lower conversion rates. This adjustment might prevent a potential {demo_improvement_rate}% decrease in conversion rate."

# Recommendations for Use Case 2: Keyword Analysis for Interest Targeting
high_ctr_keywords = df.sort_values('CTR', ascending=False).head(1)
keyword_improvement_rate = 8  # Assume an 8% improvement in conversion rate

recommendation_keywords_1 = f"Focus on keywords related to '{high_ctr_keywords['AdText'].iloc[0]}' for higher click-through rates and engagement. This adjustment could potentially lead to an {keyword_improvement_rate}% increase in conversion rate."

low_ctr_keywords = df.sort_values('CTR').head(1)
recommendation_keywords_2 = f"Avoid keywords related to '{low_ctr_keywords['AdText'].iloc[0]}' as they have lower click-through rates. This adjustment might prevent a potential {keyword_improvement_rate}% decrease in conversion rate."

# Recommendations for Use Case 3: Seasonal Performance Analysis
high_seasonal_performance = df.groupby('Season', as_index=False).agg({
    'Clicks': 'mean',
    'Conversions': 'mean',
    'CTR': 'mean',
    'Conversion Rate': 'mean'
}).sort_values('Conversion Rate', ascending=False).head(1)

seasonal_improvement_rate = 12  # Assume a 12% improvement in conversion rate

recommendation_seasonal_1 = f"Focus more on the '{high_seasonal_performance['Season'].iloc[0]}' season for higher conversions. This adjustment could potentially result in a {seasonal_improvement_rate}% increase in conversion rate."

low_seasonal_performance = df.groupby('Season', as_index=False).agg({
    'Clicks': 'mean',
    'Conversions': 'mean',
    'CTR': 'mean',
    'Conversion Rate': 'mean'
}).sort_values('Conversion Rate').head(1)

recommendation_seasonal_2 = f"Avoid allocating significant budget to the '{low_seasonal_performance['Season'].iloc[0]}' season as it has lower conversion rates. This adjustment might prevent a potential {seasonal_improvement_rate}% decrease in conversion rate."

# Print recommendations with context
print("\nRecommendations with Context:")
print("\n**Identifying High-Performing Demographics**")
print(recommendation_demo_1)
print(recommendation_demo_2)

print("\n**Keyword Analysis for Interest Targeting**")
print(recommendation_keywords_1)
print(recommendation_keywords_2)

print("\n**Seasonal Performance Analysis**")
print(recommendation_seasonal_1)
print(recommendation_seasonal_2)
# Assuming keywords_df is the DataFrame you want to export
