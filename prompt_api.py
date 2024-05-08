import openai
import requests
api_key = "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"
openai.api_key = api_key
def response_function(url,function):
    json = print(requests.post(url + function).json())
    return json
def get_choice_text_from_prompt(messages):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-4-turbo",
            messages=messages,
            temperature=0,
            max_tokens=4000
        )
        choice_text = response.choices[0]["message"]["content"]
        #print("Response is :",response)
        
        return choice_text
    except Exception as e:
        print("Error in get_choice_text_from_prompt:", str(e))
        return ""
    
def api_docs(url):
    api_json = url + "/openapi.json"
    response = requests.get(api_json)
    api_docs = response.text
    return api_docs

    
def get_url(url, question, api_docs):
    try:
        system = """
        You are an excellent NLP engineer, and your task is to analyze a FastAPI URL with some  POST functions. Your goal is to hit the right function based on the provided question .

        The system instructions are:

        Step-1:
        Based on the given question, identify the appropriate POST function from the FastAPI URL and return it.API documentation also given is along with it for identifying the right function name.
        For example:
        If ""/increase_bidrate_keywords":{"post":{"summary":"Increase Bidrate Keywords","operationId":"increase_bidrate_keywords_increase_bidrate_keywords_post","responses":{"200":{"description":"Successful Response","content":{"application/json":{"schema":{}}}}}}}"
        this is the schema in api_docs, then the function will be "/increase_bidrate_keywords" and the "operationId" which is "increase_bidrate_keywords_increase_bidrate_keywords_post" is not the function name and do not return the "operationId". Accordingly identify the correct function.
        and return the function name based on the question provided.
        The above function is only an example, you should identify the correct function based on the question.
        Then return only the name.No need to add any extra sentences along with the function name.
    
        Finally return the function name based on the question provided.

        Step-2:
        Only return the function name based on the question provided,nothing else.
        """

        prompt = f"""
        System instructions: {system}
        Only return the function name, not the "oerationId" from the FastAPI URL based on the question given,no need of additional information.
        The URL, the question, and API documentation are given inside text delimited by triple backticks.

        FastAPI URL: ```{url}```
        Question: ```{question}```
        API Documentation: ```{api_docs}```
        """

        messages = [
            {'role': 'system', 'content': system},
            {'role': 'user', 'content': prompt}
        ]

        function = get_choice_text_from_prompt(messages)
        print(function)
        json = response_function(url,function)
        return json

    except Exception as e:
        print(f"Error Extracting the URL: {e}")
        return ""


url = "https://version-demogpt-dot-ga4-bq-connector.el.r.appspot.com/"
question = "How the campaign evaluated based on the budget"

api_docs = api_docs(url)
result  = get_url(url,question,api_docs)
print(result)
