import logging
import azure.functions as func
import json
import os
from azure.storage.blob import BlobServiceClient

#
# Azure Blob Integration
#
graph_connection_string = os.environ["AzureGraphStorage"]
graph_container = os.environ["AzureGraphContainer"]

blob_service_client = BlobServiceClient.from_connection_string(conn_str=graph_connection_string)
graph_container_client = blob_service_client.get_container_client(container=graph_container)

def upload_blob(img_temp_file,target_file,properties):
    metadata = { "parent_document_name": base64.encodebytes(bytes(properties[0],'utf8')).decode("utf-8")}
    blob_client = graph_container_client.upload_blob(name=target_file, data=img_temp_file, metadata=metadata, overwrite=True)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Graphout Python HTTP trigger function processed a request.')

    try:
        body = json.dumps(req.get_json())
    except ValueError:
        return func.HttpResponse(
             "Invalid body",
             status_code=400
        )
   
    if body:
        result = compose_response(body)
        return func.HttpResponse(result, mimetype="application/json")
    else:
        return func.HttpResponse(
             "Invalid body",
             status_code=400
        )

def compose_response(json_data):
    # Receive input data
    in_data = json.loads(json_data)
    if 'values' in in_data:
        documents = in_data['values']
    elif 'documents' in in_data:
        documents = in_data['documents']
    # Prepare the Output before the loop
    results = {}
    results["values"] = []
    # Through each document    
    for document in documents:
        output_record = transform_value(document)
        if output_record != None:
            results["values"].append(output_record)
    return json.dumps(results, ensure_ascii=False)

## Perform an operation on a record
def transform_value(value):
    try:
        if 'recordId' in value:
            recordId = value['recordId']
        elif 'id' in value:
            recordId = value['id']
    except AssertionError  as error:
        return None

    data = value 

    if 'data' in value: 
        data = value['data']

    if not data :
        return ({
                "recordId": recordId,
                "data": {
                    "text": "no data found."
                        }
                })

    try:
        # Get the document key 

        #   {
        #     "text": "Contoso Steakhouse",
        #     "type": "Location",
        #     "subtype": null,
        #     "offset": 11,
        #     "length": 18,
        #     "score": 0.46
        #   },

        # NER
        if 'entities' in data:
            # Create a vertex per KP
            for entity in entities:
                

                pass
            pass
          
        # Linked Entities
        if 'linked_entities' in data:
            # Create a vertex per KP
            for entity in entities:
                pass
            pass
            
        # Relations
        if 'relations' in data:
            # Create a vertex per KP
            for rel in relations:
                pass
            pass

        # PII Entities
        if 'pii_entities' in data:
            # Create a vertex per KP and link it to the document
            pass
        
        # Azure Redis Cache to store 

        concatenated_string = "All Entities have been graph'ed."  

        upload_blob
    except:
        return (
            {
            "recordId": recordId,
            "errors": [ { "message": "Could not complete operation for record." }   ]       
            })

    return ({
            "recordId": recordId,
            "data": {
                "text": concatenated_string
                    }
            })

