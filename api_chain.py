from langchain.chains import APIChain
from langchain_openai import OpenAI
import os
import requests

os.environ["OPENAI_API_KEY"] = "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"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
url = "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/"

# Use requests to fetch the content of the URL
response = requests.get("https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/openapi.json")
api_docs = response.text
llm = OpenAI(temperature=0)
headers = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
   "Content-Type": "application/json",
}
chain = APIChain.from_llm_and_api_docs(
    llm,
    api_docs=api_docs,
    verbose=True,
    headers=headers,
    limit_to_domains=None
,
)
chain.run(
    "How the campaign evaluated based on the budget from the above url?"
)
