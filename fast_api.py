from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import openai
import requests

app = FastAPI()

class QuestionInput(BaseModel):
    url: str
    question: str

api_key = "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"
openai.api_key = api_key

def api_docs(url):
    api_json = url + "/openapi.json"
    response = requests.get(api_json)
    api_docs = response.text
    return api_docs

def response_function(url, function):
    try:
        json_response = requests.post(url + function).json()
        return json_response
    except Exception as e:
        print(f"Error in response_function: {e}")
        return {}

def extract_function_name(url, question, api_docs):
    try:
        # Construct the prompt for OpenAI
        system = """
          You are an excellent NLP engineer, and your task is to analyze a FastAPI URL with some  POST functions. Your goal is to hit the right function based on the provided question .

        The system instructions are:

        Step-1:
        Based on the given question, identify the appropriate POST function from the FastAPI URL and return it.API documentation also given is along with it for identifying the right function name.
        For example:
        If ""/increase_bidrate_keywords":{"post":{"summary":"Increase Bidrate Keywords","operationId":"increase_bidrate_keywords_increase_bidrate_keywords_post","responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{}}}}}}}"
        this is the schema in api_docs, then the function name will be "/increase_bidrate_keywords" and the "operationId" which is "increase_bidrate_keywords_increase_bidrate_keywords_post" is not the function name and do not return the "operationId". Accordingly identify the correct function
        and return the function name based on the question provided.
        The above function is only an example, you should identify the correct function based on the question.
        Then return only the name.No need to add any extra sentences along with the function name.
    
        Finally return the function name based on the question provided.

        Step-2:
        Only return the function name based on the question provided,nothing else.
        """
        prompt = f"""
        System instructions: {system}
        Only return the function name , not the "operationId" from the FastAPI URL based on the question given, no need of additional information.
        The URL, the question, and API documentation are given inside text delimited by triple backticks.

        FastAPI URL: ```{url}```
        Question: ```{question}```
        API Documentation: ```{api_docs}```
        """

        # OpenAI ChatCompletion API call
        messages = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': prompt}
        ]
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo-16k",
            messages=messages,
            temperature=0,
            max_tokens=4000
        )
        
        # Extract function name from OpenAI response
        function_name = response.choices[0]["message"]["content"].strip()
        return function_name
    except Exception as e:
        print(f"Error extracting function name: {e}")
        return ""

@app.post("/evaluate")
def evaluate_question(input_data: QuestionInput):
    try:
        api_docs_text = api_docs(input_data.url)

        function_name = extract_function_name(input_data.url, input_data.question, api_docs_text)
        result_json = response_function(input_data.url, function_name)

        return {"function_name": function_name, "result": result_json}
    except Exception as e:
        print(f"Error in evaluate_question: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
