import sys, os, joblib, logging
sys.path.insert(1, '../')
from datetime import datetime
from abc import ABC, abstractmethod    
from util.openmetadataApi import OpenMetadataConnection
from utils import loggerFunction
    
logger = loggerFunction()     

class DataFetcher(ABC):
    '''
    A class used to fetch data from the datasources based on cloudevent query.
    '''
    def __init__(self):
        self.conn = OpenMetadataConnection()
    
    def checkCacheAvailable(self,dataSource,cacheQueryDetails):
        """
        Check if cacheData present or not

        Parameters
        ----------
        dataSource: str
            Datasource name
        
        cacheQueryDetails:
            Query details
            
        Return 
        ------
        cacheData: dict or FileNotFoundError                  
            Cache data loaded from the directory
        """
        try:
            logger.info('Cache found and loaded.')
            cacheData = joblib.load('./cache/cache.joblib')

        except FileNotFoundError:
            logger.info('Creating cache..')
            cacheData = FileNotFoundError
        return cacheData
    
    def checkCacheDataExpiry(self,expiryTime,dataTimestamp):
        """
        This method checks if the cached datta is expired or not. If expired, then refresh it or load from cache and return it.

        Parameters
        ----------
        expiryTime: str
            Number of days for expiry
            
        dateTimestamp: str
            Date timestamp when data was fetched from the API
        Return 
        ------
        booleanVar: bool                  
            True: Cache expired
            False: Not expired
        """
        publishedDate = datetime.fromisoformat(dataTimestamp)
        currentDate = datetime.now()

        formatCurrentDateTime = datetime.fromisoformat(str(currentDate))

        publishedDateSplit = str(publishedDate).split(' ')
        currentDateSplit = str(currentDate).split(' ')

        publishedDateProcessing1 = datetime.strptime(publishedDateSplit[0],"%Y-%m-%d")
        currentDateProcessing1 = datetime.strptime(currentDateSplit[0],"%Y-%m-%d")

        delta = currentDateProcessing1 - publishedDateProcessing1
        deltaSplit = str(delta).split(" ")
        # print(type(deltaSplit[0]),deltaSplit,delta)
        if int(deltaSplit[0]) >= int(expiryTime):
            return True
        else:
            return False
  
    def dataPush(self,pushMessage):
        """
        This function push data to ElasticSearch 
        Parameters
        ----------
        pushMessage: dict
            Message to save to database
            
        Return
        ------
        data: dict
            Return response obtained from elasticsearch
        """
        data = self.conn.post("/v1/person",pushMessage)
        return data

    
    def dataUpdate(self,dataSourceIndex,documentId, updateMessage):
        '''This method will be used to query the data-sources based on their API requirements.'''
        return NotImplemented

    @abstractmethod
    def dataFormatter(self,queryCloudEvent,dummyResponse):
        return NotImplemented
    
    @abstractmethod
    def queryBuilder(self,queryCloudEvent):
        '''This method will be used to query the data-sources based on their API requirements.'''
        return NotImplemented

    @abstractmethod
    def dataFetch(self,queryCloudEvent):
        '''
        This method receives the queryCloudEvent and selects the data source for fetching 
        '''
        return NotImplemented