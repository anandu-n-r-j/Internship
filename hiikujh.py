from langchain_core.callbacks.manager import RunManager
import requests
import os 
from langchain_openai import OpenAI
from langchain.chains.api.openapi.requests_chain import APIRequesterChain
from langchain.utilities.requests import TextRequestsWrapper

os.environ["OPENAI_API_KEY"] = "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"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
url = "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/"

# Use requests to fetch the content of the URL
response = requests.get("https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/openapi.json")
api_docs = response.text
llm = OpenAI(temperature=0)
question =  "How the campaign evaluated based on the budget from the above url?"
api_url = APIRequesterChain.predict(
    question=question,
    api_docs=api_docs,
    callbacks=RunManager.get_child(),
)
api_response = TextRequestsWrapper.get(api_url)