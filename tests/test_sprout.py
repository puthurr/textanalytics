# tests/test_httptrigger.py
import unittest
import json
import sys
import requests
import logging

class TestFunction(unittest.TestCase):
    def test_my_function(self):
        logging.info('test_my_function')
        with open('test_input_sprout.json') as json_file:
            data = json.load(json_file)
        logging.info(data)
        url = 'http://localhost:7071/api/sprout'
        x = requests.post(url, json = data)
        print(x.text)
        
        # Check the output.
        self.assertEqual(x.status_code,200)

if __name__ == '__main__':
    unittest.main()