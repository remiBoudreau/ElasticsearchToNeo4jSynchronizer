from logging import Logger
from typing import Union, str, List, Dict
from elasticsearch import Elasticsearch

class ElasticSearchHandler:
    def __init__(self, 
                hosts: Union[str, List[str]], 
                username: str, 
                password: str, 
                caCerts: str, 
                caFingerprint:str, 
                index: str, 
                logger: Logger):
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
            index: str
                The name of the Elasticsearch index to search.
            logger: Logger
                The logging object to use for error reporting.
            """
            try:
                self.index = index
                self.logger = logger

                # ElasticSearch Connection
                self.client = Elasticsearch(
                    hosts=hosts or ['localhost:9200'],
                    http_auth=(username, password),
                    ca_certs=caCerts,
                    ssl_assert_fingerprint=caFingerprint,
                    verify_certs=bool(caCerts or caFingerprint)
                )
            except Exception as e:
                self.logger.error(f"Failed to connect to Elasticsearch: {e}")
                
    def dataFetch(self, query: dict) -> Union[dict, None]:
        """
        This function takes the Elasticsearch query generated in queryBuilder and retrieves the data from the Elasticsearch index.

        Parameters
        ----------
        query : dict
            A dictionary containing the Elasticsearch query parameters.

        Returns
        -------
        dataFetchResponse : dict or None
            A dictionary containing the search results. If no results are found, returns None.
        """
        dataFetchResponse = {}
        try:
            dataFetchResponse = self.client.search(index=self.index, query=query)
        except Exception as e:
            dataFetchResponse["error"] = (f"Failed to retrieve data from Elasticsearch: {e}")
        finally: 
            if "error" in dataFetchResponse:
                self.logger.error(f"Failed to retrieve data from Elasticsearch: {dataFetchResponse['error']}")
        return dataFetchResponse