import os
import datetime
import requests
import logging
from requests.exceptions import HTTPError
from enum import Enum
import re
from thefuzz import fuzz

LOG_USE_STREAM = os.environ.get("LOG_USE_STREAM", True)
LOG_PATH = os.environ.get("LOG_PATH", '/var/log/psi/checkmate')
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()

logging.basicConfig(level=LOGLEVEL)
logger = logging.getLogger(__name__)
if LOG_USE_STREAM:
    handler = logging.StreamHandler()
else:
    now = datetime.datetime.now()
    handler = logging.FileHandler(
                LOG_PATH 
                + now.strftime("%Y-%m-%d") 
                + '.log')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class CatalogEntity(str, Enum):
    PERSON = "person"
    ORGANIZATION = "organization"
    THING = "thing"

class APIRequestMethod(Enum):
    POST = 'Create'
    PUT = 'Update'
    DELETE = 'Delete'
    GET = 'Get'

class OpenMetadataConnection:
    def __init__(self, 
                 uri=os.getenv("CATALOG_SERVER", "http://catalog-server:8585/api"), 
                 user_id=os.getenv("CATALOG_CLIENT_ID", "3469758d-e7f4-407e-b1e8-d8738ddb5ced")):
        
        self.__catalog_uri = uri if not uri.endswith('/') else uri[-1]
        self.__user_id = user_id
        self.__token = None
        self.__token_expiration = None
        self._getToken(user_id=user_id)
        
    def _getToken(self, user_id=None):
        if user_id != None:
            self.__user_id = user_id
        if self.__user_id == None:
            logger.error(f'You must specify an UserID to connect to Openmetadata')
            return
        response = self.get(f'/v1/users/token/{self.__user_id}',tokenPresent=False)
        if response == None:
            logger.error(f'Error retrieving token')
            return
            
        if 'JWTToken' in response:
            self.__token = response['JWTToken']
            self.__token_expiration = datetime.datetime.fromtimestamp(response['JWTTokenExpiresAt']/1000)
            #response = self.get(f'/v1/users/{self.__user_id}')
            if self.__token_expiration < datetime.datetime.now(): #Token expired
                logger.info('Refreshing expired token.')
                self._generateNewToken()
            else:
                logger.debug('Token retrieved.')
        else:
            self._generateNewToken()
    
    def _generateNewToken(self):
        payload = {
                    "JWTTokenExpiry": "7"
                    }
        response = self.put(f'/v1/users/generateToken/{self.__user_id}',payload,tokenPresent=False)
        if response == None:
            logger.error(f'Error retrieving token')
            return
        logger.debug(f'New token generated')
        self.__token = response['JWTToken']
        self.__token_expiration = datetime.datetime.fromtimestamp(response['JWTTokenExpiresAt']/1000)
        
    def put(self, url_path,json_payload=None, tokenPresent=True):
        return self._build_request(APIRequestMethod.PUT,f"{self.__catalog_uri}{url_path}",json_payload,tokenPresent=tokenPresent)
    
    def get(self, url_path,tokenPresent=True):
        return self._build_request(APIRequestMethod.GET,f"{self.__catalog_uri}{url_path}",tokenPresent=tokenPresent)
    
    def post(self, url_path,json_payload=None,tokenPresent=True):
        return self._build_request(APIRequestMethod.POST,f"{self.__catalog_uri}{url_path}",json_payload,tokenPresent=tokenPresent)
    
    def _build_request(self, type, api_url, json_payload=None, tokenPresent=True):
        headers ={}
        if tokenPresent:
            if self.__token == None or self.__token_expiration < datetime.datetime.now():
                self._getToken()
            headers = {"Authorization": f"Bearer {self.__token}"}
        try:
            response = None
            if type == APIRequestMethod.GET:
                response = requests.get(url=api_url,headers=headers)
            elif type == APIRequestMethod.POST:
                if json_payload == None:
                    response = requests.post(url=api_url, headers=headers)
                else:
                    response = requests.post(url=api_url, headers=headers ,json = json_payload)
            elif type == APIRequestMethod.PUT:
                if json_payload == None:
                    response = requests.put(url=api_url, headers=headers)
                else:
                    response = requests.put(url=api_url, headers=headers ,json = json_payload)
            else:
                logger.error(f'Undefined HTTP Method: {type}')
                return None
            if not response.ok:
                logger.error(f'Api request failed: {response.text} URL:{api_url}, json={json_payload}')
                return None
        except HTTPError as http_err:
            logger.error(f'HTTP error occurred: {http_err}. URL:{api_url}, json={json_payload}')  # Python 3.6
            return None
        except Exception as err:
            logger.error(f'Other error occurred: {err} for {api_url}')  # Python 3.6
            return None
        else:
            logger.debug(f'Api request succeeded.')
        return response.json()

def get_name_match_scores(catalogName, entityName):
        enl=entityName.lower()
        # Very stringent because no other comparison than name
        threshold = int(os.getenv('NAME_MATCHING_THRESHOLD', 80))
        pattern = r"\s*[\(\[].*?[\)\]]"
        catalogName = re.sub(pattern, "", catalogName)

        eNames=catalogName.lower().split(" ")
        highestMatch=0

        if (len(eNames)>=2):
            nameVersion=eNames[-1]+" "+eNames[0]
            highestMatch = fuzz.ratio(nameVersion,enl)

        for fNameItems in range(len(eNames)):
            nameVersion=" ".join(eNames[0:fNameItems])+" "+eNames[-1]
            matchLevel = fuzz.ratio(nameVersion,enl)
            if matchLevel>highestMatch:
                highestMatch = matchLevel
                if highestMatch>threshold:
                    logger.debug(f'Name match scores {catalogName} x {entityName} = {True} {highestMatch}')
                    return True,highestMatch
        if highestMatch>threshold:
            logger.debug(f'Name match scores {catalogName} x {entityName} = {True} {highestMatch}')
            return True,highestMatch
        logger.debug(f'Name match scores {catalogName} x {entityName} = {False} {highestMatch}')
        return False,highestMatch

# TODO: convert node from dict to Node
def retrieveCatalogEntity(connection: OpenMetadataConnection, node:dict, nodeType):
    name = node['name'].lower()
    data_check = connection.get(f'/v1/search/query?q={name}&index={nodeType.lower()}&expand=false')
    catalog_name = data_check["nodes"][0]["name"].lower()
    # Names Matcher Returns 1 almost always if name is remotely similar.
    if len(data_check["nodes"]) > 0 and get_name_match_scores(catalog_name,name)[0] and nodeType.lower() != 'thing':
        entity = data_check["nodes"][0]
        entity['existent'] = True                    
    elif len(data_check["nodes"]) > 0 and catalog_name == name:
        entity = data_check["nodes"][0]
        entity['existent'] = True
    else:
        #new_entity = {"aggregateRating": 0.0} 
        new_entity = {} 
        new_entity.update(node)
        response = connection.post(f"/v1/{nodeType.lower()}",new_entity)
        entity = response
        if response == None:
            return response
        entity['existent'] = False
    return entity