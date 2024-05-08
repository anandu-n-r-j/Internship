from fastapi import FastAPI, HTTPException
from typing import List
import openai
from google.cloud import bigquery
import os

# Initialize FastAPI app
app = FastAPI()

# Set your OpenAI API key
api_key = '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'     
openai.api_key = api_key

# Initialize BigQuery client
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account-key.json"
client = bigquery.Client.from_service_account_json(
    os.getenv("GOOGLE_APPLICATION_CREDENTIALS"), 
    project='ga4-bq-connector'
)

def fetch_column_values(source_table, column_name):
    """
    Fetch distinct values of a column from the source table
    """
    query = f"SELECT DISTINCT {column_name} FROM `{source_table}`"
    result = client.query(query).to_dataframe()

    # Extract the values from the DataFrame and convert to a list
    values_list = result[column_name].tolist()
    return values_list

def get_choice_text_from_prompt(messages):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-16k",
            messages=messages,
            temperature=0,
            max_tokens=4000
        )
        choice_text = response.choices[0]["message"]["content"]
        return choice_text
    except Exception as e:
        print("Error in get_choice_text_from_prompt:", str(e))
        return ""

def get_new_keywords(keywords):
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
        for keyword in keywords:
            prompt += f"- {keyword}\n"

        messages = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': prompt}
        ]

        similar_keywords = get_choice_text_from_prompt(messages)

        # Parsing the response into a dictionary
        result_dict = {}
        for line in similar_keywords.split('\n'):
            if ':' in line:
                key, value = line.split(':')
                result_dict[key.strip()] = value.strip()

        return result_dict
    except Exception as e:
        print(f"Error getting the similar keywords: {e}")
        return {}

def get_table_ddl(project_id, dataset_id, table_name):
    
    query = f"""
        SELECT ddl 
        FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = '{table_name}'
    """

    # Executing the query
    query_job = client.query(query)

    # Extracting the DDL statement from the query results
    ddl_statement = None
    for row in query_job:
        ddl_statement = row['ddl']
        break  # Assuming there's only one row

    return ddl_statement

def create_new_table_with_schema(dataset_id, new_table_name, ddl):
    # Define the dataset and table reference
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(new_table_name)

    # Extract schema lines from the DDL statement
    schema_lines = ddl.split("\n")
    start_index = schema_lines.index("(")
    end_index = schema_lines.index(")")
    schema_lines = schema_lines[start_index + 1:end_index]

    # Define the schema based on the extracted schema lines
    schema = []
    for line in schema_lines:
        line = line.strip()  # Remove leading and trailing whitespace
        if line:  # Skip empty lines
            parts = line.split(None, 2)  # Split line into parts: name, type, description
            field_name = parts[0]
            field_type = parts[1]
            description = parts[2][10:-1] if len(parts) > 2 else None  # Extract description from options
            schema.append(bigquery.SchemaField(field_name, field_type, description=description))

    # Define the table object with the schema
    table = bigquery.Table(table_ref, schema=schema)

    # Create the new table
    client.create_table(table)
    print(f"New table {dataset_id}.{new_table_name} created with schema from DDL statement.")

def replace_keywords(text, keyword_dict):
    for old_keyword, new_keyword in keyword_dict.items():
        text = text.replace(old_keyword, new_keyword)
    return text

def upload_data_with_keyword_replacement(project_id,source_dataset, source_table, target_dataset, target_table, keyword_dict):


    # Fetch data from the source table
    source_table_ref = client.dataset(source_dataset).table(source_table)
    source_data = client.list_rows(source_table_ref).to_dataframe()

    # Replace keywords in the 'ad_group_criterion_keyword_text' column
    source_data['ad_group_criterion_keyword_text'] = source_data['ad_group_criterion_keyword_text'].apply(lambda x: replace_keywords(x, keyword_dict))

    # Upload data to the target table
    target_table_ref = client.dataset(target_dataset).table(target_table)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite if table exists
    job = client.load_table_from_dataframe(source_data, target_table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print(f"Data uploaded to {project_id}.{target_dataset}.{target_table} with keyword replacement.")


@app.post("/process_data/")
def process_data(project_id: str, source_dataset: str, source_table: str, target_dataset: str, target_table: str):
    try:
        # Fetch column values
        column_name = "ad_group_criterion_keyword_text"
        keywords_list = fetch_column_values(f"{project_id}.{source_dataset}.{source_table}", column_name)

        # Get new keywords
        keyword_dict = get_new_keywords(keywords_list)

        # Get DDL for old table
        ddl = get_table_ddl(project_id, source_dataset, source_table)

        # Create new table with schema
        create_new_table_with_schema(target_dataset, target_table, ddl)

        # Upload data with keyword replacement
        upload_data_with_keyword_replacement(project_id, source_dataset, source_table, target_dataset, target_table, keyword_dict)

        return {"message": "Data processing completed successfully!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

