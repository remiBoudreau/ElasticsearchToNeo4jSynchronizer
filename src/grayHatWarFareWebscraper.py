import ast, math, logging, os, json, requests, sys
from requests.exceptions import HTTPError
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk 
from tika import parser
from urllib.error import HTTPError
import nltk
from datetime import datetime
sys.path.insert(1, '../../sdk/data-ingress-common/')
sys.path.insert(1, '../../sdk/')
from dataFetcher import DataFetcher
from dataSources import DataSources
from nodeType import NodeType
from utils import loggerFunction

logger = loggerFunction()                               # logger 

class grayHatWarfareBucketsScraper(DataFetcher):
    """
    This class inherits DataFetcher class. This class fetches the articles URLs using the google-api-client and pass the 
    URLs to internal data scraper module to retreive the text from the articles.

    Attributes
    ----------
    DataFetcher : Parent class 
        Parent class that defines the class structure for other dataFetcher (or dataSources) classes 

    Methods
    -------
    queryBuilder (queryCloudEvent: dict)
        Develops the search queries from taxonomy cloudevent to get the articles related to the query keywords
        
    dataPush (pushMessage: dict)
        This function push data to database (Elasticsearch)
    
    dataFormatter (queryCloudEvent: dict, messageContent: dict)
        Formats the response data according to ElasticSearch JSON schema format
    
    getArticleLinks (seachQuery: str)
        Fetch all the articles URLs related to the searchQuery

    dataFetch (queryCloudEvent: dict articleURL: url)
        Get the articles text data using internal data scraper 

    startProcess (queryCloudEvent: dict)
        Function should fetch data, save it in ElasticSearch and pass to the payload to next module
    """
    def __init__(self):
        super().__init__()

    def queryBuilder(self, queryCloudEvent: dict):
        """
        This method develops the search query from the taxonomy cloudevent that is used to fetch the related articles from the internet
        Note:Below is an example query
        
        Parameters
        ----------
        queryCloudEvent : dict
            This cloudevent has taxonomy details required to prepare a search Query to fetch data from Webz.io. E.g. Refer to webz.io payload in "runner.py"

        Return 
        ------
        searchQuery: str
            Search query string used to get related articles URLs from the internet
        """
        searchQuery = [{
            taxonomyContents.get('subject'): {
                propertyContents.get('subject'): propertyContents["value"]
            }
        }   for taxonomyContents in queryCloudEvent['searchQueries'] for key, value in taxonomyContents.items() \
            if (key == 'key' and value == 'name' and taxonomyContents.get('subject') is not None) \
            
            for propertyContents in taxonomyContents['properties'] \
            if (propertyContents.get('key') == "ConstraintType.STARTSWITH" and propertyContents.get('subject') is not None)] 
        
        gseQueries = []
        for subject in searchQuery.keys():
            if subject == NodeType.PERSON:
                #query 1: Tom Hanks 
                #query 2: Tom Hanks employer
                gseQueries.extend([
                    f"{searchQuery[subject]['name']}",   
                    f"{searchQuery[subject]['name']} employer",
                ])
                if 'email' in searchQuery[subject]:
                    #query: Tom Hanks tom@hollywood.com
                    gseQueries.append(f"{searchQuery[subject]['name']} {searchQuery[subject]['email']}")
                if 'address' in searchQuery[subject]:
                    #query: Tom Hanks Los Angeles, CA
                    gseQueries.append(f"{searchQuery[subject]['name']} {searchQuery[subject]['address']}")
            elif subject == NodeType.ORGANIZATION:
                # query: Apple inc 
                # instead of just apple
                gseQueries.append(f"{searchQuery[subject]['name']} inc")
                if 'address' in searchQuery[subject]:
                    #query: Apple Cupertino, CA
                    gseQueries.append(f"{searchQuery[subject]['name']} {searchQuery[subject]['address']}")
            else:
                gseQueries.append(f"{searchQuery[subject]['name']}")
        
        return gseQueries if gseQueries else None
    
    def dataPush(self, pushMessage):
        """
        This function push data to database (Elasticsearch)
        Parameters
        ----------
        pushMessage: dict
            Message to save to database
            
        Return
        ------
        elasticSearchId: str
            Document UUID obtained from elasticsearch 
        """
        data = self.conn.put("/v1/search/document", pushMessage)
        if type(data) == dict:
            return data['id']
        elif type(data) == str:
            return data
        else:
            logger.error(f'Bad return type ({type(data)}): {data}')
        return data
        
    def dataFormatter(self, queryCloudEvent: dict, messageContent: dict):
        """
        Prepare formatted ElasticSearch payload to dump in ElasticSearch

        Parameters
        ----------
        queryCloudEvent: dict
            This cloudevent has taxonomy details 

        messageContent: dict 
            Content of the articles (E.g. Text from the article)

        Return 
        ------
        elasticSearchPayload: dict
            Elasticsearch schema formatted payload (contains articles data and taxonomy information)
        """
        tags = []
        filters = []
        categories = []

        for jsonContent in queryCloudEvent['searchQueries']:
            if jsonContent['key'] == 'taxonomy-id':
                taxonomyId = jsonContent['value']
            elif jsonContent['key'] == 'tenant-name':
                tenantName = jsonContent['key']
            elif jsonContent['key'] == 'search-id':
                searchId = jsonContent['value']
            elif jsonContent['key'] == "expansion-query-id":
                expansionId = jsonContent['value']
            elif jsonContent['key'] == 'name':
                tags.append(jsonContent['value'])
                filters.append(jsonContent['properties'])
                categories.append(jsonContent['subject'])
        
        sourceId = "grayHatWarfareBucketsScraper"                                                            
        # print(type(messageContent))
        elasticSearchPayload = {'source':f'{DataSources.DATASCRAPER.value}',
                                'sourceId': f'{sourceId}',
                                'requestTime': f"{datetime.today().strftime('%Y-%m-%d')}",                  # Time when request is sent. add datetime.now() in datafetch function
                                "docId": "UUID",
                                "duration": "Transit time taken by request",
                                "TTL": "22",                                         # Time the record will exist before expire(considering days right now)Note:limit is 30 days
                                "searchId": f'{searchId}',
                                "correlationId":f'{searchId}',                              # Track event through all services
                                "taxonomyName": "String",                            
                                'taxonomyId':f'{taxonomyId}',
                                'searchType': 'ENUM STRING',
                                'tenantType':f'{tenantName}',
                                'searchTerm': grayHatWarfareBucketsScraper.queryBuilder(self,queryCloudEvent),
                                
                                'data': json.dumps({
                                            'data-key': messageContent
                                            }),
                                
                                'tags':tags,
                                'filters': filters,
                                'Categories':categories
                                }
        return elasticSearchPayload

    def generateURLs(    
                self,             
                api_key = '',
                query ='', 
                extensions = '',
                start=0,
                limit=1000
                ):
        
        """
        This method queries a search term against the Grayhathacker bucket api and creates a generator function yielding the urls for documents containing the search term (or None if the search returned no results).
        
        Parameters
        ----------
        api_key     : str
            Grayhathacker bucket api key
        
        query       : str
            query string to search against document names in grayHatWarfare's open buckets
        
        extensions  : str
            String of extensions in a comma-seperated list restricting the search to only match for files with those extensions; e.g.'jpg, png, tiff'

        Yield 
        ------
        ( hit | None ) : ( dict | None ) 
            Generator function for individual urls containing files
        """
        urls = []
        n_results=9999999999999999999999
        while start < 2000: # n_results + limit:
            # Query Buckets API
            url = f"https://buckets.grayhatwarfare.com/api/v1/files/[{query}/{start}/{limit}?access_token={api_key}"
            if extensions:
                url = f"{url}&extensions={extensions}" 
            hits = requests.get(url).json()
            end = str(start + limit) if n_results > (start + limit) else str(n_results)
            try:
                print(hits['results'])
            except:
                print(hits)
            print(f'parsing results from: {start} to: {end}')
            # No server response errors
            if 'error' not in hits and len(hits['files']) > 0:
                # Generator for url
                for hit in hits['files'][0:5]:
                    try:
                        urls.append(hit['url'].replace(" ", "%20"))
                    except KeyError:
                        print(f'Shoot! No URL could not be parsed for the file: {hit}. Skipping this file...')
                        pass
                break
            # Errors/no hits
            else:
                try:
                    print(f"Oops! Looks like grayhatwarfare's bucket API had an error: The {hits['_content'].decode('utf-8')['error']}")
                except KeyError:
                        print('Shoot! No documents with ' \
                            f'extensions: {extensions}' if extensions else '' \
                            f'has a name that contains the string {query}'
                            )
            start += limit
        return urls
    
    def generateDocuments(
            self,
            urls_generator
        ):
        """
        This method fetches the pdf from a url and parses the text
        
        Parameters
        ----------
        url_generator: str
            Generator function for individual urls containing files

        Yield 
        ------
        ( document | None ) : ( dict | None )
            Generator function for individual documents fetching and parsing (or None)
        """
        # 
        for url in urls_generator:
            document = {}
            if url is not None:
                try:
                    document = parser.from_file(url)
                except HTTPError:
                    pass
                except AttributeError:
                    pass
                yield document

            else: 
                yield document
                


    def startProcess(self, queryCloudEvent: dict, index: str, extensions: str, limit: str):
        """
        This method is a runner function which will be checking all the possible condition, will scrape the file data from the grayhatwarfare bucket api and dump in ES

        Parameters
        ----------
        queryCloudEvent: dict
            This cloudevent has taxonomy details required to prepare a search Query to fetch data 

        Return 
        ------
        payLoadList: list                  
            List of payloads. Each payload contains article text data and other taxonomy related details.
        """
        ELASTIC_HOST = os.getenv('ELASTIC_HOST')
        ELASTIC_USERNAME = os.getenv('ELASTIC_USERNAME')
        ELASTIC_PASSWORD = os.getenv('ELASTIC_PASSWORD')
        CA_CERTS = os.getenv('CA_CERTS')
        CERT_FINGERPRINT = os.getenv('CERT_FINGERPRINT')
        INDEX = os.getenv('INDEX')
        API_KEY = os.getenv('API_KEY')

        if index is not None:
            payLoadList = []
            queries = self.queryBuilder(queryCloudEvent)
            if queries is None:
                logger.error(f'Error parsing Search query on payload: {queryCloudEvent}')
                return None
            elif len(queries) == 0:
                logger.warn(f'No search query not found on payload: {queryCloudEvent}')

        urlList = set()
                
        urls = urlList.update(self.generateURLs(
            api_key=API_KEY, 
            query=queryCloudEvent, 
            extensions=extensions, 
            limit=limit)
        )

        
        for url in urls:
            fileContent = self.dataFetch(queryCloudEvent, url)
            elasticSearchPayload = self.dataFormatter(queryCloudEvent, fileContent)
            elasticSearchId = self.dataPush(elasticSearchPayload)
            dataDict = ast.literal_eval(elasticSearchPayload["data"])
            dataDict["documentId"] = elasticSearchId
            elasticSearchPayload["data"] = dataDict
            payLoadList.append(elasticSearchPayload)
        return payLoadList
    