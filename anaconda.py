import pandas as pd
import numpy as np

# Replace 'filename.csv' with the path to your CSV file
df = pd.read_csv('/Users/raki/Quantacus/CustomGPT Demo/CustomGPTDemo/campaignSOS.csv')



# Convert 'date' column to datetime
df['segments_date'] = pd.to_datetime(df['segments_date'])

# Identify metrics and dimensions
metrics = [col for col in df.columns if col.startswith('metrics')]
dimensions = [col for col in df.columns if not col.startswith('metrics') and col != 'date']

# Convert non-numeric values to NaN
df['conversion_value'] = pd.to_numeric(df['conversion_value'], errors='coerce')
df['metrics_cost_micros'] = pd.to_numeric(df['metrics_cost_micros'], errors='coerce')

# Fill NaN values with zero
df.fillna(0, inplace=True)

# Now, you can perform the groupby operation and calculate ROI
grouped_data = df.groupby('campaign_id').apply(
    lambda data: data['conversion_value'].sum() / (data['metrics_cost_micros'].sum() / 1e6)
).reset_index(name='ROI')


# Convert metrics to float and dimensions to category
for metric in metrics:
    df[metric] = df[metric].astype(float)

for dimension in dimensions:
    df[dimension] = df[dimension].astype('category')

# Group the data by 'campaign_id' and calculate ROI
grouped_data = df.groupby('campaign_id').apply(
    lambda data: data['conversion_value'].sum() / (data['metrics_cost_micros'].sum() / 1e6)
).reset_index(name='ROI')

print(grouped_data.head())