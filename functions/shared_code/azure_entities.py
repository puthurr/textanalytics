import os
import hashlib
import logging
import json

DOCUMENT_LABEL = 'document'
DOCUMENT_ENTITY_EDGE_LABEL = 'refersto'
DOCUMENT_LINKED_ENTITY_EDGE_LABEL = 'linksto'
LINKED_ENTITY_LABEL = 'linked_entity'

AZURE_TEXT_ANALYTICS_NER = 0
AZURE_TEXT_ANALYTICS_LINKED_ENTITIES = 1
AZURE_TEXT_ANALYTICS_PII_PHI = 2
AZURE_TEXT_ANALYTICS_FOR_HEALTH = 3

AZURE_ENTITIES_SOURCES = [ AZURE_TEXT_ANALYTICS_NER, AZURE_TEXT_ANALYTICS_LINKED_ENTITIES, AZURE_TEXT_ANALYTICS_PII_PHI, AZURE_TEXT_ANALYTICS_FOR_HEALTH]

# Load default Configuration
mapping = {}
json_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "mapping.json")
with open(json_file_path) as json_file:
    mapping=json.loads(json_file.read())

MAPPING_VERTEX="vertex"
MAPPING_EDGE="edge"

def azure_entities_extractor(source_id, source_label, data):
    vertices = []
    edges = []

    try:
        # NER OR Linked Entities 
        if 'entities' in data:
            entities = data['entities']
            if 'matches' in entities[0]:
                # Linked Entities 
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
                # Create a vertex per KP
                for entity in entities:
                    # Vertex
                    vertex = map_entity(entity,MAPPING_VERTEX)
                    vertex['label']= LINKED_ENTITY_LABEL
                    vertices.append(vertex)

                    # Edge
                    edge = map_entity(entity,MAPPING_EDGE)
                    edge['id']= hashlib.md5((source_id+'source_label_'+entity['id']).encode('utf-8')).hexdigest()
                    edge['label']= DOCUMENT_LINKED_ENTITY_EDGE_LABEL
                    edge['source']= source_id
                    edge['sourcelabel']= source_label
                    edge['target']= entity['id']
                    edge['targetlabel']= LINKED_ENTITY_LABEL

                    # Properties
                    edge['offset']=entity['matches'][0]['offset']
                    edge['score']=entity['matches'][0]['score']
                    for match in entity['matches']:
                        # Avg the offset and score ? 
                        # Avg distance between matches - distribution
                        pass
                    # TODO - Add last modified time + creation time of the document
                    edges.append(edge)
            else:
                # NER 
                    #   {
                    #     "text": "Contoso Steakhouse",
                    #     "type": "Location",
                    #     "subtype": null,
                    #     "offset": 11,
                    #     "length": 18,
                    #     "score": 0.46
                    #   },
                # Create a vertex per KP
                for entity in entities:
                    vertex = map_entity(entity,MAPPING_VERTEX)
                    vertices.append(vertex)
                    # 
                    edge = map_entity(entity,MAPPING_EDGE)
                    edge['id']= hashlib.md5((source_id+'source_label _'+entity['text']).encode('utf-8')).hexdigest()
                    edge['label']= DOCUMENT_ENTITY_EDGE_LABEL
                    edge['source']= source_id 
                    edge['sourcelabel']= source_label

                    edges.append(edge)
           
        # Relations
        if 'relations' in data:
            # Create a vertex per KP
            for rel in data['relations']:
                pass
            pass

    except Exception as ex:
        logging.error(ex)
        return (vertices,edges)

    return (vertices,edges)

def map_entity(entity,mapping_type):
    dict = {}
    for map in mapping[mapping_type]:
        for key in map.keys():
            value = map_property(entity,map[key])
            if value:
                dict[key]=value
    return dict

def map_property(entity, map_entry):   
    for value in map_entry:
        if value in entity:
            return entity[value]
    return None
