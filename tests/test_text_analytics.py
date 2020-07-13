# tests/test_httptrigger.py
import unittest
import json
import sys
import requests
import logging

from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential

key = "<paste-your-text-analytics-key-here>"
endpoint = "<paste-your-text-analytics-endpoint-here>"

def authenticate_client():
    ta_credential = AzureKeyCredential(key)
    text_analytics_client = TextAnalyticsClient(
            endpoint=endpoint, credential=ta_credential)
    return text_analytics_client

client = authenticate_client()

# def entity_recognition_example(client):

#     try:
#         documents = ["I had a wonderful trip to Seattle last week."]
#         result = client.recognize_entities(documents = documents)[0]

#         print("Named Entities:\n")
#         for entity in result.entities:
#             print("\tText: \t", entity.text, "\tCategory: \t", entity.category, "\tSubCategory: \t", entity.subcategory,
#                     "\n\tConfidence Score: \t", round(entity.confidence_score, 2), "\n")

#     except Exception as err:
#         print("Encountered exception. {}".format(err))
# entity_recognition_example(client)

class TestFunction(unittest.TestCase):
    def test_my_function(self):
        logging.info('test_my_function')
        with open('test_input_graphout.json') as json_file:
            data = json.load(json_file)
        logging.info(data)
        url = 'http://localhost:7071/api/graphout'
        x = requests.post(url, json = data)
        print(x.text)
        
        # Check the output.
        self.assertEqual(x.status_code,200)

if __name__ == '__main__':
    unittest.main()