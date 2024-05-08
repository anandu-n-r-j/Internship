'''
# For Models
from langchain_community.llms import OpenAI
#from langchain_community.chat_models import ChatOpenAI
from langchain_openai import ChatOpenAI
# For Agent and Tools
from langchain.agents import AgentType
from langchain.agents import initialize_agent, Tool
from langchain_core.utils.function_calling import convert_to_openai_function
from langchain_community.tools import format_tool_to_openai_function, BaseTool
from pydantic import BaseModel, Field
from typing import Optional, Type
import json
import requests
# For Message schemas,
from langchain.schema import HumanMessage, AIMessage, ChatMessage, FunctionMessage

from langchain_community.tools import APIOperation
# Import OpenAI key
import os
os.environ["OPENAI_API_KEY"] = "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"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Set model name for this notebook: gpt-3.5-turbo
model_name = "gpt-3.5-turbo"

def campaign_evaluation_based_on_budget():
api_url = "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/"
response = requests.post(api_url + "campaign_evaluation_based_on_budget")
campaign_data = response.json()
print(campaign_data)

from langchain.chains.openai_functions.openapi import get_openapi_chain

chain = get_openapi_chain(
    "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/openapi.json"
)
chain("Evaluate the campaign based on the budget")





class CampaignEvaluationTool(BaseTool):
    name = "campaign_evaluation_based_on_budget"
    description = "Used to evaluate a campaign based on the provided budget"

    def _generate_scenario_description(self, row):
        scenario = row.get('scenario', 'Unknown Scenario')
        reasoning = row.get('reasoning', 'N/A')

        return scenario, reasoning

    def _run(self):
        
        api_url = "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/"
        response = requests.post(api_url + "campaign_evaluation_based_on_budget")
        campaign_data = response.json()

        # Extract relevant information from the API response
        scenario, reasoning = self._generate_scenario_description(campaign_data)

        # Use LangModel to generate a description based on the scenario
        description_generator = ChatOpenAI(model=model_name, temperature=0)
        generated_description = description_generator.generate(reasoning, max_tokens=100)

        result = {"campaign_data": campaign_data, "description": generated_description, "scenario": scenario}
        return result
    
    args_schema: Optional[Type[BaseModel]] = None  # No need for the args_schema

# Usage
tools = [CampaignEvaluationTool()]
functions = [convert_to_openai_function(tool_name) for tool_name in tools]

query = "What is the result of the data ?"
model = ChatOpenAI(
            model = model_name, 
            temperature = 0,
        )

# ... (previous code)

# Define message types
human_message = HumanMessage(content=query)
ai_message = AIMessage(content="")  # Empty content for AI message
function_message = FunctionMessage(name='campaign_evaluation_based_on_budget', content="")

# Invoke the model
response_ai_message = model.invoke([human_message, ai_message], functions=functions)

# Extracting arguments from the response_ai_message
_args = json.loads(response_ai_message.additional_kwargs['function_call'].get('arguments'))

# Run the tool
tool_result = tools[0](_args)

# Update the content of the AI message with the tool result
ai_message.content = str(tool_result)

# Final prediction
response_final = model.predict_messages(
    [human_message, ai_message, function_message],
    functions=functions
)

print(response_final)
'''

'''
from langchain_community.tools.openapi.utils.api_models import APIOperation
from langchain.chains.openai_functions.openapi import get_openapi_chain

chain = get_openapi_chain(
    "https://www.klarna.com/us/shopping/public/openai/v0/api-docs/"
)
chain("What are some options for a men's large blue button down shirt")
'''
from langchain_community.chains.openai_functions.openapi import get_openapi_chain

# Download the OpenAPI spec to a local file (e.g., openapi_spec.json)
# Use a tool like wget or curl to download the spec.
# For example: wget https://www.klarna.com/us/shopping/public/openai/v0/api-docs/ -O openapi_spec.json

chain = get_openapi_chain("openapi_spec.json")

question = "What are some options for a men's large blue button down shirt"
inputs = {"query": question}
result = chain(inputs)
print(result)

