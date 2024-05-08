import os
import requests
from langchain.agents import initialize_agent
os.environ["OPENAI_API_KEY"] = "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"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
from langchain import OpenAI 
from langchain.chat_models import ChatOpenAI
from langchain.chains.conversation.memory import ConversationBufferWindowMemory
turbo_llm = ChatOpenAI(
    temperature=0,
    model_name='gpt-3.5-turbo'
)

from langchain.agents import Tool
def extract_results_from_url(url):
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    print(f"Response Content: {response.text}")
    if response.status_code == 200:
        results_json = response.json()
        return results_json
    else:
        return {"error": f"Failed to fetch data from URL. Status code: {response.status_code}"}


url_tool = Tool(
    name='URL Extractor',
    func=extract_results_from_url,
    description="Extracts results from a given URL and returns them in JSON format."
)


# conversational agent memory
memory = ConversationBufferWindowMemory(
    memory_key='chat_history',
    k=3,
    return_messages=True
)


tools = [url_tool]
conversational_agent = initialize_agent(
    agent='chat-conversational-react-description',
    tools=tools,
    llm=turbo_llm,
    verbose=True,
    #max_iterations=3,
    #early_stopping_method='generate',
    memory=memory
)

fixed_prompt = '''Assistant is a large language model trained by OpenAI.
Assistant is designed to be able to provide results in json format,explanations and discussions from the url: "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/".
For every question provided, Assistant should be able to extract all the content based on the question from the corresponding url only
and return the results in json format and also provide the description aloing with it.
So the expected results is like this:
json extracted from the url bnased on the question
Descrtiption about the url things
'''
conversational_agent.agent.llm_chain.prompt.messages[0].prompt.template = fixed_prompt
conversational_agent("Evaluate the campaign based on the budget")
