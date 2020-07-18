import logging
import azure.functions as func
import json
import os
from azure.storage.blob import BlobServiceClient
import uuid
import hashlib
 
from ..shared_code import azure_entities as ae

#
# Azure Blob Integration
#
graph_connection_string = os.environ["AzureGraphStorage"]
graph_container = os.environ["AzureGraphContainer"]

blob_service_client = BlobServiceClient.from_connection_string(conn_str=graph_connection_string)
graph_container_client = blob_service_client.get_container_client(container=graph_container)

def upload_blob(img_temp_file,target_file,metadata):
    try:
        blob_client = graph_container_client.upload_blob(name=target_file, data=img_temp_file, metadata=metadata, overwrite=True)
        pass
    except Exception as ex:
        logging.error(ex)
        pass

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
    values = json.loads(json_data)['values']  
    # Prepare the Output before the loop
    results = {}
    results["values"] = []
    
    for value in values:
        output_record = transform_value(value)
        if output_record != None:
            results["values"].append(output_record)
    return json.dumps(results, ensure_ascii=False)

## Perform an operation on a record
def transform_value(value):
    try:
        recordId = value['recordId']
    except AssertionError  as error:
        return None

    if 'data' in value: 
        data = value['data']

        vertices = []

        # Root vertex to link all entities to
        vertex = { }
        vertex['id']=recordId
        vertex['label']=ae.DOCUMENT_LABEL
        vertex['type']=ae.DOCUMENT_LABEL
        vertices.append(vertex)

        edges = []

        try:
            (evertices,eedges) = ae.azure_entities_extractor(recordId,ae.DOCUMENT_LABEL,data)

            vertices.extend(evertices)
            edges.extend(eedges)              
            
            concatenated_string = str.format("Created {0} vertices and {1} edges.",len(vertices),len(edges))

            upload_blob(json.dumps(vertices),recordId+".vertices.json",{})
            upload_blob(json.dumps(edges),recordId+".edges.json",{})
            logging.info(vertices)
            logging.info(edges)
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
    else:
        return ({
                "recordId": recordId,
                "data": {
                    "text": "no data found."
                        }
                })

