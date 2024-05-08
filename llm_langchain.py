import requests
import yaml
import os
from langchain.requests import RequestsWrapper
from langchain_openai import OpenAI
from langchain.agents.agent_toolkits.openapi import planner
import json

os.environ["OPENAI_API_KEY"] = "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"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
llm = OpenAI(temperature=0, model_name="gpt-3.5-turbo")
url = "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/openapi.json"

r = requests.get(url)
r.raise_for_status()

# Save the JSON content to a file
with open("openapi_from_url2.json", 'w') as f:
    f.write(r.text)

# Convert JSON to YAML and move 'endpoints' inside 'info'
with open("openapi_from_url2.json") as json_file:
    json_data = json.load(json_file)
    yaml_data = {'info': {'endpoints': json_data.pop('paths')}}
    
with open("openapi_from_url2.yaml", 'w') as yaml_file:
    yaml.dump(yaml_data, yaml_file, default_flow_style=False)

os.remove("openapi_from_url2.json")  # Remove the JSON file if not needed

os.rename("openapi_from_url2.yaml", "openapi_from_url2.yaml")

with open("openapi_from_url2.yaml") as f:
    raw_open_api_spec = yaml.load(f, Loader=yaml.Loader)

if 'info' in raw_open_api_spec and 'endpoints' in raw_open_api_spec['info']:
    # Rename 'paths' to 'endpoints' in the OpenAPI spec
    raw_open_api_spec['endpoints'] = raw_open_api_spec['info'].pop('endpoints')

print(raw_open_api_spec)

headers = {
    "Authorization": f"Bearer {os.getenv('OPEN_API_KEY')}" 
}

openai_requests_wrapper = RequestsWrapper(headers=headers)

# Check if 'endpoints' is in the raw_open_api_spec
if 'endpoints' in raw_open_api_spec:
    # Pass 'endpoints' to create_openapi_agent
    openai_agent = planner.create_openapi_agent(raw_open_api_spec, openai_requests_wrapper, llm)

    query = "Evaluate the campaigns based on the budget"
    openai_agent.run(query)
else:
    print("No 'endpoints' found in the OpenAPI specification.")
