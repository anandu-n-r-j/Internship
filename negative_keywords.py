import pandas as pd
import numpy as np

# Simulated data for organic skincare keywords and extended performance metrics
num_keywords = 30

keywords_data = {
    'KeywordID': [f'KW{i}' for i in range(1, num_keywords + 1)],
    'KeywordText': ['organic skincare', 'natural beauty products', 'pure skincare', 'eco-friendly cosmetics',
                    'clean beauty', 'non-toxic skincare', 'green beauty', 'vegan skincare', 'cruelty-free products',
                    'sustainable beauty', 'plant-based skincare', 'holistic skincare', 'organic anti-aging',
                    'clean skincare routine', 'ethical beauty', 'green cosmetics', 'organic beauty essentials',
                    'toxin-free skincare', 'earth-friendly beauty', 'organic sunscreen', 'cleanse and detox skincare',
                    'organic spa products', 'pure and simple skincare', 'green skincare rituals',
                    'organic beauty regimen', 'mindful skincare', 'natural glow beauty', 'conscious beauty',
                    'organic skincare for sensitive skin', 'nourishing plant extracts'],
    'Clicks': np.random.randint(10, 150, size=num_keywords),
    'Impressions': np.random.randint(500, 5000, size=num_keywords),
    'Conversions': np.random.randint(2, 20, size=num_keywords),
    'Cost': np.random.uniform(5, 50, size=num_keywords)  # Adjusted cost per keyword for realism
}

# Manually set 5 keywords to meet the criteria
keywords_data['KeywordText'][0] = 'chemical skincare'
keywords_data['KeywordText'][2] = 'artificial skincare'
keywords_data['KeywordText'][4] = 'synthetic skincare'
keywords_data['KeywordText'][6] = 'organic skincare'
keywords_data['KeywordText'][8] = 'natural skincare'

keywords_df = pd.DataFrame(keywords_data)

# Add currency as USD to each keyword
keywords_df['Currency'] = 'USD'

# Calculate CTR and Conversion Rate
keywords_df['CTR'] = keywords_df['Clicks'] / keywords_df['Impressions'] * 100
keywords_df['Conversion Rate'] = (keywords_df['Conversions'] / keywords_df['Clicks']) * 100

# Display the modified simulated keywords dataset with realistic cost values, currency, CTR, and Conversion Rate
pd.options.display.float_format = '{:.2f}%'.format
print("Simulated Keywords Dataset:")
print(keywords_df)

# Undesired terms in the negative keywords list
undesired_terms = ['chemical', 'artificial', 'synthetic']

# Adjust the thresholds based on your specific criteria
click_threshold = 50
conversion_threshold = 5
ctr_threshold = 0.5  # Adjusted CTR threshold
conversion_rate_threshold = 2.0

# Identify potential negative keywords with reasoning
potential_negative_keywords = keywords_df[
    ((keywords_df['Clicks'] < click_threshold) & (keywords_df['Conversions'] < conversion_threshold)) |
    (keywords_df['KeywordText'].apply(lambda x: any(term in x for term in undesired_terms))) |
    (keywords_df['CTR'] < ctr_threshold) |
    (keywords_df['Conversion Rate'] < conversion_rate_threshold)
]

# Add a 'Reasoning' column based on the identified criteria
potential_negative_keywords['Reasoning'] = ''

# Provide reasoning based on the criteria for each potential negative keyword
potential_negative_keywords.loc[potential_negative_keywords['Clicks'] < click_threshold, 'Reasoning'] += 'Low Clicks. '
potential_negative_keywords.loc[potential_negative_keywords['Conversions'] < conversion_threshold, 'Reasoning'] += 'Low Conversions. '
potential_negative_keywords.loc[potential_negative_keywords['CTR'] < ctr_threshold, 'Reasoning'] += 'Low CTR (< 0.5%). '
potential_negative_keywords.loc[potential_negative_keywords['Conversion Rate'] < conversion_rate_threshold, 'Reasoning'] += 'Low Conversion Rate. '
potential_negative_keywords.loc[keywords_df['KeywordText'].apply(lambda x: any(term in x for term in undesired_terms)), 'Reasoning'] += 'Undesired Term. '

# Display the potential negative keywords with realistic cost values, currency, CTR, Conversion Rate, and reasoning
print("\nPotential Negative Keywords:")
print(potential_negative_keywords[['KeywordID', 'KeywordText', 'Clicks', 'Conversions', 'Cost', 'Currency', 'CTR', 'Conversion Rate', 'Reasoning']])

# Assuming keywords_df is the DataFrame you want to export
keywords_df.to_csv('/home/attom_corp/simulated_demo_data1/keywords_data.csv', index=False)