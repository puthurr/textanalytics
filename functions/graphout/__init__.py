import logging
import azure.functions as func
import json
import os
from azure.storage.blob import BlobServiceClient
import uuid
import hashlib  
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
    # # Validate the inputs
    # try:         
    #     assert ('data' in value), "'data' field is required."
    #     data = value['data']        
    #     assert ('text1' in data), "'text1' field is required in 'data' object."
    #     assert ('text2' in data), "'text2' field is required in 'data' object."
    # except AssertionError  as error:
    #     return (
    #         {
    #         "recordId": recordId,
    #         "errors": [ { "message": "Error:" + error.args[0] }   ]       
    #         })
        data = value['data']

        vertices = []

        DOCUMENT_LABEL = 'document'
        DOCUMENT_ENTITY_EDGE_LABEL = 'refersto'
        DOCUMENT_LINKED_ENTITY_EDGE_LABEL = 'linksto'
        LINKED_ENTITY_LABEL = 'linked_entity'

        vertex = { }
        vertex['id']=recordId
        vertex['label']=DOCUMENT_LABEL
        vertices.append(vertex)

        edges = []

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
                for entity in data['entities']:
                    vertex = { }
                    vertex['id']=entity['text']
                    vertex['label']= entity['type']
                    vertex['name']=entity['text']
                    vertices.append(vertex)
                    for prop in ['subtype','length']:
                        vertex[prop]=entity[prop]
                        pass
                    # 
                    edge = { }
                    edge['id']= hashlib.md5((recordId+'_'+entity['text']).encode('utf-8')).hexdigest()
                    edge['label']= DOCUMENT_ENTITY_EDGE_LABEL
                    edge['source']= recordId
                    edge['sourcelabel']= DOCUMENT_LABEL
                    edge['target']= entity['text']
                    edge['targetlabel']= entity['type']
                    edge['offset']=entity['offset']
                    edge['score']=entity['score']
                    # TODO - Add last modified time + creation time of the document

                    edges.append(edge)
                    pass
                pass
                
        #     "name": "Sirloin steak",
        #     "matches": [
        #       {
        #         "text": "Sirloin steak",
        #         "offset": 346,
        #         "length": 13,
        #         "score": 0.69
        #       }
        #     ],
        #     "language": "en",
        #     "id": "Sirloin steak",
        #     "url": "https://en.wikipedia.org/wiki/Sirloin_steak",
        #     "datasource": "Wikipedia"

            # Linked Entities
            if 'linked_entities' in data:
                # Create a vertex per KP
                for entity in data['linked_entities']:
                    # Vertex
                    vertex = { }
                    vertex['label']= LINKED_ENTITY_LABEL
                    for prop in ['id','name','language','url','datasource']:
                        vertex[prop]=entity[prop]
                        pass
                    vertices.append(vertex)
                    # Edge
                    edge = { }
                    edge['id']= hashlib.md5((recordId+'_'+entity['id']).encode('utf-8')).hexdigest()
                    edge['label']= DOCUMENT_LINKED_ENTITY_EDGE_LABEL
                    edge['source']= recordId
                    edge['sourcelabel']= DOCUMENT_LABEL
                    edge['target']= entity['id']
                    edge['targetlabel']= LINKED_ENTITY_LABEL
                    edge['offset']=entity['matches'][0]['offset']
                    edge['score']=entity['matches'][0]['score']
                    for match in entity['matches']:
                        # Avg the offset and score ? 
                        # Avg distance between matches - distribution
                        pass
                    # TODO - Add last modified time + creation time of the document
                    edges.append(edge)
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

