from logging import Logger
from typing import Union, str, List, Dict
from elasticsearch import Elasticsearch

class ElasticSearchHandler:
    def __init__(self, 
                hosts: Union[str, List[str]], 
                username: str, 
                password: str, 
                ca_certs: str, 
                ca_fingerprint:str, 
                index: str, 
                logger: Logger,
                KeyErrorHandler: function):
            """
            Constructor method creates an Elasticsearch client instance.

            Parameters
            ----------
            hosts: str or list of str
                The Elasticsearch host or list of hosts to connect to. Defaults to 'localhost:9200'.
            username: str
                The username to authenticate with. Defaults to None.
            password: str
                The password to authenticate with. Defaults to None.
            ca_certs: str
                The path to the CA certificates file. Defaults to None.
            ca_fingerprint: str
                The SHA-256 fingerprint of the CA certificate. Defaults to None.
            """
            try:
                self.index = index
                self.logger = logger
                # ElasticSearch Connection
                self.client = Elasticsearch(
                    hosts=hosts or ['localhost:9200'],
                    http_auth=(username, password),
                    ca_certs=ca_certs,
                    ssl_assert_fingerprint=ca_fingerprint,
                    verify_certs=bool(ca_certs or ca_fingerprint)
                )
            except Exception as e:
                self.logger.error(f"Failed to connect to Elasticsearch: {e}")

    def queryBuilder(self, queryCloudEvent, subjectFields, selectedProperty):
        """
        This function extracts the person name or company name that you need for search.

        Parameters
        ----------
        queryCloudEvent: dict
            This cloudevent has taxonomy details required to prepare a search Query to fetch data from ES index

        Return 
        ------
        search_query: dict
            A dictionary containing the Elasticsearch query parameters.
        """
        try:
            # Build the search query
            must_queries = [{
                "multi_match": {
                    "query": property['value'].lower(),
                    "fields": subjectFields[event_search_query['subject']],
                    "operator": "and",
                    "fuzziness": "AUTO"
                }
            } for event_search_query in queryCloudEvent['searchQueries'] 
            for property in event_search_query['properties'] 
            if property['key'] == selectedProperty]

            # Combine the must queries with a bool query
            search_query = {"bool": {"must": must_queries}}
            
            return search_query
        except KeyError as e:
            self.logger.error(f'Failed to parse queryEventHandler: {e}')

    def datFetch(self, elasticsearch_query, entity_keys):
        """
        This function takes the elasticsearch_query generated in queryBuilder and retrieves the data from the Elasticsearch index.

        Parameters
        ----------
        elasticsearch_query : dict
            A dictionary containing the Elasticsearch query parameters.
        entity_keys : list of str
            A list of keys to extract from the indiviudal documents' data obtained from the Elasticsearch search result. THey are the fields that contain entities.

        Returns
        -------
        dataFetchResponse : dict or None
            A dictionary containing the search results. If no results are found, returns None.
        """
        try:
            dataFetchResponse = self.client.search(index=self.index, query=elasticsearch_query)
        except Exception as e:
            self.logger.error(f"Failed to retrieve data from Elasticsearch: {e}")
            return None

        if 'hits' not in dataFetchResponse or not dataFetchResponse['hits']['hits']:
            return None

        for key in entity_keys:
            for hit in dataFetchResponse['hits']['hits']:
                if key in hit['_source']:
                    hit['_source'][key] = [dict(item) for item in hit['_source'][key]]

        return dataFetchResponse
    
    def elasticsearchMapper(dataFetchResponse):
        keyDependencyMap = ['hits', 'hits', '_source']   
        keyDependencyMapTypes = [list, list, list]
        keyDependencyGenerator = ((key[0], self.KeyErrorHandler(key[0], key[1])) for key in keyDependencyMap)
        return 

    def elasticSearchOperations(self, queryCloudEvent, subjectFields, selectedProperty, entity_keys):
        elasticsearchQuery = self.queryBuilder(queryCloudEvent, subjectFields, selectedProperty)                #1
        dataFetchResponse = self.dataFetch(elasticsearch_query=elasticsearchQuery, entity_keys=entity_keys) #2
        return dataFetchResponse
    
    def createElasticSearchPipeline(self, subjectFields, selectedProperty, searchQueries):
        return map(lambda searchQuery: self.elasticSearchOperations(searchQuery, subjectFields, selectedProperty), searchQueries)
    

