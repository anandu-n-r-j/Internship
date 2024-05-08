from fastapi import FastAPI
import openai
from google.cloud import bigquery
import os

# Initializing FastAPI app
app = FastAPI()

# Setting your OpenAI API key
api_key = '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'     
openai.api_key = api_key

# Initializing BigQuery client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account-key.json"
client = bigquery.Client.from_service_account_json(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
    project='ga4-bq-connector'
)

@app.post("/processing table and uploading the data")
def process_table():
    # Fetching distinct values of the column 'ad_group_criterion_keyword_text' from the source table
    query = f"SELECT DISTINCT ad_group_criterion_keyword_text FROM `ga4-bq-connector.TDKGoogleAdsDataset.p_ads_Keyword_2222881946`"
    result = client.query(query).to_dataframe()

    # Extracting the values from the DataFrame and convert to a list
    values_list = result['ad_group_criterion_keyword_text'].tolist()

    # Generating new similar keywords for the fetched values
    try:
        system = """
        You will be provided with a list containing some keywords, and you have to generate new similar keywords for these words.
        You have to generate similar keywords for the given keywords and return the results in dictionary format.
        Step 1:
        Analyze the given keywords and generate new similar words for each given word.
        Step 2:
        Return the keywords in the JSON format like below:
        {given_keyword: new_generated_keyword}
        """
        # Constructing the prompt with given keywords
        prompt = "Keywords to be replaced:\n"
        for keyword in values_list:
            prompt += f"- {keyword}\n"

        # Call the OpenAI API to get the response
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-16k",
            messages=[
                {'role': 'system', 'content': system},
                {'role': 'user', 'content': prompt}
            ],
            temperature=0,
            max_tokens=4000
        )
        
        # Extract choice text from the response
        choice_text = response.choices[0]["message"]["content"]

        # Parsing the response into a dictionary
        result_dict = {}
        for line in choice_text.split('\n'):
            if ':' in line:
                key, value = line.split(':')
                result_dict[key.strip()] = value.strip()

    except Exception as e:
        print(f"Error generating new keywords: {e}")

    # Fetching DDL statement from the original table
    ddl_query = f"""
        SELECT ddl 
        FROM `ga4-bq-connector.TDKGoogleAdsDataset.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = 'p_ads_Keyword_2222881946'
    """
    ddl_job = client.query(ddl_query)
    ddl_statement = None
    for row in ddl_job:
        ddl_statement = row['ddl']
        break

    # Extracting schema from DDL statement
    schema_lines = ddl_statement.split("\n")
    start_index = schema_lines.index("(")
    end_index = schema_lines.index(")")
    schema_lines = schema_lines[start_index + 1:end_index]

    schema = []
    for line in schema_lines:
        line = line.strip()
        if line:
            parts = line.split(None, 2)
            field_name = parts[0]
            field_type = parts[1]
            description = parts[2][10:-1] if len(parts) > 2 else None
            schema.append(bigquery.SchemaField(field_name, field_type, description=description))

    # Defining the new table reference
    new_table_ref = client.dataset('DemoData').table('p_ads_Keyword')

    # Creating the new table with the extracted schema
    new_table = bigquery.Table(new_table_ref, schema=schema)
    client.create_table(new_table)
    print(f"New table DemoData.p_ads_Keyword created...")

    # Fetching data from the source table
    source_table_ref = client.dataset('TDKGoogleAdsDataset').table('p_ads_Keyword_2222881946')
    source_data = client.list_rows(source_table_ref).to_dataframe()

    # Replacing keywords in the specified column
    for old_keyword, new_keyword in result_dict.items():
        source_data['ad_group_criterion_keyword_text'] = source_data['ad_group_criterion_keyword_text'].str.replace(old_keyword, new_keyword)

    # Uploading data to the new table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(source_data, new_table_ref, job_config=job_config)
    job.result()
    print(f"Data uploaded to DemoData.p_ads_Keyword with keyword replacement.")

# Calling the function to execute the process
process_table()
