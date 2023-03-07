import orjson as json
import uuid
from enum import Enum
from datetime import datetime

class CloudEventSubject(str, Enum):
    PERSON = "person"
    ORGANIZATION = "organization"
    THING = "thing"

class CloudEventType(str, Enum):
    SEARCH = "search"
    EXPAND = "expand"
    QUERY = "query"
    
class CloudEventSource(str, Enum):
    CATALOG = "catalog"
    PIPELINE = "PIPELINE"

CLOUD_EVENT_TEMPLATE = {
    "specversion": "1.0",
    "type": "expand", #search||expand||query
    "source": "search/clientId123/people/",
    "subject": "person", #person||organization||object
    "service": "pipeline.svc",
    "id": "6e8bc430-9c3a-11d9-9669-0800200c9a66",
    "parentId" : None, # In case of expansion/recursive data flow this will be populate
    "time": "2018-04-05T17:31:00Z",
    "clientId": "UUID:Client",
    "datacontenttype": "application/json",
    "data": None
}

PAYLOAD_TEMPLATE = {
    "type": "extend",
    "term": "Tom Hanks",
    "subject": "Person",
    "token": "WDWW-RERER_.43dswsf",
    "sessionId": 85458,
    "sources": [
        "tier1db",
        "tier2db"
    ],
    "sinks": [
        "tier1db",
        "tier2db"
    ],
    "parameters": [
        {
            'key': 'name',
            'value': 'Tom Hanks',
            'type': 'property',
            'subject': "Person",
            "rel": "node",
        },
        {
            "key": "nationality.addressCountry",
            "value": "CA",
            "type": "property",
            "subject": "Person",
            "rel": "node"
        },
        {
            "key": "legalName",
            "value": "ACME Corp",
            "type": "relationship",
            "rel": "worksFor",
            "subject": "Organization"
        }
    ],
}

SEARCH_SERVICE_JSON_PAYLOAD = { "searchQueries":[
    {
        "key": "search-id", 
        "value": "dbc33a1c75fd4040b3a241cc495ec868",
        "subject": "Search"
    },
    {
        "key": "taxonomy-id",
        "value": "0fe308b87fd5412c847e0bcef60d6659",
        "subject": "Taxonomy"
    },
    {
        "key": "expansion-query-id",
        "value": "54a1ed7126774488b88034f11db70a90",
        "subject": "ExpansionQuery"
    },
    {
        "key": "name",
        "value": "taco",
        "subject": "NodeType.THING",
        "taxonomy-node-id": "17cd440338cc4e959e0f856e61b031af",
        "properties": [
            {
                "key": "ConstraintType.STARTSWITH",
                "value": "'t'",
                "subject": "node",
                "type": "property"
            }],
        "data-source": "webz.io"
    },
    {   "key":"name",
        "value": "shell",
        "subject": "NodeType.THING",
        "taxonomy-node-id": "8d368a49a5364ae089bf5c6d5e4e8719",
        "properties": [],
        "data-source": "webz.io"
    },
    {
    "key":"name",
    "value":"filling",
    "subject":"Thing",
    "taxonomy-node-id":"uuid.uuid4.hex()",
    "properties": [{"key":"endswith","value":"n","subject":"node","type":"property"}],
    "data-source": "Webz.io"
},{
    "key":"name",
    "value":"beef",
    "subject":"Thing",
    "taxonomy-node-id":"uuid.uuid4.hex()",
    "properties": [{"key":"date","value":"2022-09-12","subject":"node","type":"property"}],
    "data-source": "Webz.io"
},
{
    "key":"name",
    "value":"ground",
    "subject":"Thing",
    "taxonomy-node-id":"uuid.uuid4.hex()",
    "properties": [{"key":"startswith","value":"k","subject":"node","type":"property"}],
    "data-source": "Webz.io"
},
{
    "key":"name",
    "value":"cooked",
    "subject":"Thing",
    "taxonomy-node-id":"uuid.uuid4.hex()",
    "properties": [{"key":"startswith","value":"w","subject":"node","type":"property"}],
    "data-source": "Webz.io"
},
{
    "key":"name",
    "value":"grilled",
    "subject":"Thing",
    "taxonomy-node-id":"uuid.uuid4.hex()",
    "properties": [{"key":"startswith","value":"w","subject":"node","type":"property"}],
    "data-source": "Webz.io"
},
{
        "key": "tenant-name",
        "value": "MOCKED-ed-will-do-this-uuid",
        "subject": "Tenant"
}
]}


# def generate_event(clientId, json_payload, source=None, id=None, parentId = None, type=CloudEventType.QUERY, subject=CloudEventSubject.PERSON):
#     result = CLOUD_EVENT_TEMPLATE
#     result['id'] = str(uuid.uuid4()) if id == None else id
#     result['parentId'] = parentId
#     result['type'] = type
#     result['subject'] = subject
#     result['source'] = source if source != None else "search/clientId123/people/"
#     result['clientId'] = clientId
#     result['time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
#     result['data'] = {}
#     result['data']['value'] = list(json.dumps(json_payload))
#     return result
def generate_event(json_payload, id=None, parentId = None, clientId=None, depth = 1, type=CloudEventType.EXPAND.value, subject=CloudEventSubject.PERSON.value, source=CloudEventSource.PIPELINE.value):
    cloud_event = {}
    cloud_event['extension'] = {}
    cloud_event['time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    cloud_event['id'] = str(uuid.uuid4()) if id == None else id
    cloud_event['parentId'] = parentId
    cloud_event['subject'] = subject
    cloud_event['source'] = source
    cloud_event['extensions'] = {}
    cloud_event['extensions']['correlationid'] = cloud_event['id']
    cloud_event['extensions']['parentId'] = parentId
    cloud_event['extensions']['ttl'] = 30
    cloud_event['extensions']['depth'] = depth
    if clientId:
        cloud_event['extensions']['clientId'] = clientId
    
    json_payload['id'] = cloud_event['id']
    json_payload['searchId'] = cloud_event['id']
    json_payload['correlationId'] = cloud_event['id']
    json_payload['parentId'] = cloud_event['parentId']
    cloud_event['data'] = {}
    cloud_event['data']['value'] = list(json.dumps(json_payload))
    
    return cloud_event

def generate_from_ce(ce, json_payload):
    result = ce
    result['time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    result['data'] = {}
    result['data']['value'] = list(json.dumps(json_payload))
    return result

def generate_recursive_ce(ce, json_payload):
    result = ce
    result['parentId'] = result['id']
    result['id'] = str(uuid.uuid4()) if id == None else id
    result['type'] = "expansion"
    result['source'] = "pipeline"
    result['time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    result['data'] = {}
    result['data']['value'] = list(json.dumps(json_payload))
    result['extensions']['service'] = 'pipeline.svc'
    result['extensions']['correlationid'] = result['id']
    return result

def generate_recursive_ce(ce, json_payload):
    result = ce
    result['parentId'] = result['id']
    result['id'] = str(uuid.uuid4()) if id == None else id
    result['type'] = "expansion"
    result['source'] = "pipeline"
    result['time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    result['data'] = {}
    result['data']['value'] = list(json.dumps(json_payload))
    result['extensions']['service'] = 'pipeline.svc'
    result['extensions']['correlationid'] = result['id']
    return result

def get_event_payload(cloud_event):
    return json.loads(cloud_event['data'])