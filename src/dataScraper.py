import ast, math, logging, os, json, requests, sys
from requests.exceptions import HTTPError
from urllib.parse import urlparse
from urllib.request import urlopen
from bs4 import BeautifulSoup
import lxml, nltk
from datetime import datetime
sys.path.insert(1, '../../sdk/data-ingress-common/')
sys.path.insert(1, '../../sdk/')
from googleapiclient.discovery import build
from dataFetcher import DataFetcher
from dataSources import DataSources
from nodeType import NodeType
from languageEnum import LanguageEnum
from utils import loggerFunction
from googletrans import Translator

logger = loggerFunction()                               # logger 

CCTLD_LANGUAGE_MAP = {
    'ru': 'Russian'
}



class DataScraper(DataFetcher):
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
        nltk.download('words')
        self.english_vocab = [w.lower() for w in nltk.corpus.words.words()]

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
        searchQuery = {}
        gseQueries = []
        
        # TODO: This cam be done in one loop
        try:
            for content in queryCloudEvent:
                if content == "searchQueries":
                    for taxonomyContents in queryCloudEvent["searchQueries"]:
                        if taxonomyContents["key"] == "name":
                            # taxonomyContents["subject"] = Person|Organization
                            subject = taxonomyContents["subject"]
                            searchQuery[subject] = {}
                            for propertyContent in taxonomyContents["properties"]:
                                if propertyContent["key"] == "ConstraintType.STARTSWITH" and propertyContent["subject"] in ['name','address','email']:
                                    # propertyContent["subject"]  = name|address|email
                                    searchQuery[subject][propertyContent["subject"]] = propertyContent["value"] 
            
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
        except Exception as e:
            logger.exception(f'Unable to parse payload:{e}')
            return None
        
        #searchQuery = queryCloudEvent["searchQueries"][3]["properties"][0]["value"]  if type(queryCloudEvent['searchQueries'][3]["properties"][0]["value"]) == list else queryCloudEvent['searchQueries'][3]["properties"][0]["value"]       # can be any search terms
        #taxonomyType = queryCloudEvent["searchQueries"][3]["value"]
        #return searchQuery,taxonomyType
        return gseQueries
    
    def dataPush(self,pushMessage):
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
        data = self.conn.put("/v1/search/document",pushMessage)
        if type(data) == dict:
            return data['id']
        elif type(data) == str:
            return data
        else:
            logger.error(f'Bad return type ({type(data)}): {data}')
        return data
    
    def isWeird(self,intext:str)->bool:
        hits=0
        for token in intext.split(" "):
            if token.strip().lower() in self.english_vocab:
                hits+=1
                if hits>4:
                    return True
        return False
        
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
        
        sourceId = "dataScraper"                                                            
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
                                'searchTerm': DataScraper.queryBuilder(self,queryCloudEvent),
                                
                                'data': json.dumps({
                                            'data-key': messageContent
                                            }),
                                
                                'tags':tags,
                                'filters': filters,
                                'Categories':categories
                                }
        return elasticSearchPayload

    def getArticleLinks(self,searchQuery: str, apiKey: str,cseId: str,**kwargs):
        """
        Get the article URLs from the internet related to the search query
        Note:  Current limit of return article URLs is set 10 for testing purposes change it in production

        Parameters
        ----------
        searchQuery: str
            Search query string used to get related articles URLs from the internet

        apiKey: str 
            API key for google custom search engine (CSE)

        cseId : str                         
            Custom search engine ID 
        
        **kwargs:
            Other keyword arguments 
                - num: int
                    Maximum number of article URLs to be fetched from the google's CSE

        Return 
        ------
        articlesURLList: list
            List of the article URLs
        """
        service = build("customsearch", "v1", developerKey=apiKey)  
        num_search_results = kwargs["num"]
        #taxonomyType = kwargs["taxonomyType"]
        if num_search_results > 100:
            raise NotImplementedError('Google Custom Search API supports max of 100 results')
        elif num_search_results > 10:
            kwargs['num'] = 10 # this cannot be > 10 in API call 
            calls_to_make = math.ceil(num_search_results / 10)
        else:
            calls_to_make = 1
            
        kwargs['start'] = start_item = 1
        articlesURLList = []
        
        # Exclude linkedin results as they are blocked.
        kwargs['siteSearch']='linkedin.com'
        kwargs['siteSearchFilter']='e'
        
        # TODO translate search query
        #translatedSearchQuery = self.languageTranslator(searchQuery,fromLang=fromLang,toLang=toLang)
        
        while calls_to_make > 0:
            try:
                res = service.cse().list(q=searchQuery, cx=cseId, **kwargs).execute()
                #articlesURLList.append(res['items']["link"])
                articlesURLList.extend([item['link'] for item in res['items']])
            except KeyError as err:
                logger.warning(f'Error retrieving Google CSE ({err}):{res}')
            calls_to_make -= 1
            start_item += 10
            kwargs['start'] = start_item
            leftover = num_search_results - start_item + 1
            if 0 < leftover < 10:
                kwargs['num'] = leftover
        
        # if taxonomyType == "Person":
        #     while calls_to_make > 0:
        #         try:
        #             res = service.cse().list(q=searchQuery, cx=cseId, **kwargs).execute()
        #             articlesURLList.extend(res['items'])
                    
        #             searchString = f"Employer\ {searchQuery}"
        #             res = service.cse().list(q=searchString, cx=cseId, **kwargs).execute()
        #             articlesURLList.extend(res['items'])
        #         except KeyError:
        #             continue
        #         calls_to_make -= 1
        #         start_item += 10
        #         kwargs['start'] = start_item
        #         leftover = num_search_results - start_item + 1
        #         if 0 < leftover < 10:
        #             kwargs['num'] = leftover
        # elif taxonomyType == "Organization":                    # TODO: Confirm the "Org" or "Organization"
        #     while calls_to_make > 0:
        #         try:
        #             #searchString = f"Employer\ {searchQuery}"
        #             res = service.cse().list(q=searchQuery, cx=cseId, **kwargs).execute()
        #             articlesURLList.extend(res['items'])
        #         except KeyError:
        #             continue
        #         calls_to_make -= 1
        #         start_item += 10
        #         kwargs['start'] = start_item
        #         leftover = num_search_results - start_item + 1
        #         if 0 < leftover < 10:
        #             kwargs['num'] = leftover
        logger.info(f'Articles: {articlesURLList}')
        return articlesURLList
    
    def fetchArticleLocally(self, url, language='ENGLISH'):
        cleantext = None
        try:
            html = urlopen(url).read()
            cleantext = BeautifulSoup(html).get_text()
        except Exception as err:
            logger.exception(f'Unable to retrieve HTML or parse content {err}.')
        return {"url":url, "content": cleantext, 'language': language}
    
    def dataFetch(self,queryCloudEvent,articleURL: str,language):
        """
        Get the articles text data by using internal web-scraper
        Note: Web scraper URL is expired every two hours (so change it)

        Parameters
        ----------
        articleURL: str
            Article URL to scrape the text data

        Return 
        ------
        articleContent: dict
            Dictionary response with URL and content of the article (E.g. {"URL": value, "content": "article text"})
        """
        articleContent = None
        try:
            if 'wikipedia' in articleURL.lower():
                articleContent = self.fetchArticleLocally(articleURL)
            else:
                requestLink = os.getenv('WEB_SCRAPER_URL') +"/api/content/text?url=" + articleURL +"&language=" + language
                headers = {
                            'X-TOKEN': os.getenv('WEB_SCRAPER_AUTH_TOKEN'),
                            }

                response = requests.get(requestLink,headers=headers)      #verify=False to remove SSL verification
                if not response.ok:
                    logger.error(f'Webscraper request failed: {response.text} URL:{requestLink}')
                    return None
                # print(scrapedContent.text)
                
                articleContent = ast.literal_eval(response.content.decode('UTF-8'))
            
            ccTLD = urlparse(articleURL).netloc.split('.')[-1]
            if ccTLD in CCTLD_LANGUAGE_MAP:
                articleContent["content"] = self.languageTranslator(articleContent["content"],fromLang=CCTLD_LANGUAGE_MAP[ccTLD],toLang='English')
            
            try:
                clean_text = articleContent["content"].replace('"', '').replace("/", " ").replace("#", " ").replace('\u300c', ' ')
                clean_text = clean_text if self.isWeird(clean_text) else " "
                cleaned_text = ''.join(char for char in clean_text if ord(char) < 128) 
                articleContent["content"] = cleaned_text
            except TypeError as err:
                logger.error(f'Please check connection to webscraper: {err}')
        except HTTPError as http_err:
            logger.error(f'HTTP error connection to webscraper occurred: {http_err}. URL:{requestLink}')  # Python 3.6
            return None
        except Exception as err: 
            articleContent = "None"
            logger.exception(f'Please check connection to webscraper: {err} : {requestLink}')
        
        return articleContent
    
    def languageTranslator(self,text,fromLang=None,toLang='English'):
        """
        Language Translation function

        Parameters
        ----------
        text: str
            Text content
        
        fromLang: str
            Source language (i.e. current language of the text)
        
        toLang: str
            Destination language (i.e expected language of the text)
            
        Return
        ------
        elasticSearchId: str
            Document UUID obtained from elasticsearch 
        """
        logger.info(f'language translator: {fromLang} -> {toLang}')
        if fromLang == toLang:
            return text
        try:
            translator= Translator()
            translated_text = translator.translate(text, src=fromLang,dest=toLang)
        except RuntimeError as err :
            logger.exception(f"Check the from_lang and to_lang parameters:{err}")
            return text
        except Exception as err :
            logger.exception(f"Error translating file:{err}")
            return text
        return translated_text.text

    def startProcess(self,queryCloudEvent: dict):
        """
        This method is a runner function which will be checking all the possible condition, will scrape the article data from the internet and dump in ES

        Parameters
        ----------
        queryCloudEvent: dict
            This cloudevent has taxonomy details required to prepare a search Query to fetch data 
            
        fromLang: str
            Source language
        
        toLang: str
            Translation language or final language

        Return 
        ------
        payLoadList: list                  
            List of payloads. Each payload contains article text data and other taxonomy related details.
        """
        fromLang = "English"
        toLang = "English"
        #TODO: Check in cloudevent for fromLang and toLang parameters
        payLoadList = []
        #searchQuery,taxonomyType = self.queryBuilder(queryCloudEvent)
        queries = self.queryBuilder(queryCloudEvent)
        if queries == None:
            logger.error(f'Error parsing Search query on payload: {queryCloudEvent}')
            return None
        if len(queries) == 0:
            logger.warn(f'No search query not found on payload: {queryCloudEvent}')
        #if taxonomyType == None:
        #    logger.warning(f'Taxonomy not found for payload (using Person): {queryCloudEvent}')
        #    taxonomyType='Person'
        
        # translatedSearchQuery = self.languageTranslator(searchQuery,fromLang=fromLang,toLang=toLang)
        # print("translated search query",translatedSearchQuery)
        articleList = set()
                
        for query in queries:
            for query in queries:
                    articleList.update(
                        self.getArticleLinks(
                            query,
                            os.getenv('GOOGLE_CSE_API_TOKEN'),
                            os.getenv('GOOGLE_CSE_SEARCH_ENGINE_ID'),
                            num=int(os.getenv('NUM_OF_ARTICLES')),
                            lr=LanguageEnum[toLang.upper()].value)
                    )
        #articleList = self.getArticleLinks(translatedSearchQuery,os.getenv('GOOGLE_CSE_API_TOKEN'),os.getenv('GOOGLE_CSE_SEARCH_ENGINE_ID'),taxonomyType,num=int(os.getenv('NUM_OF_ARTICLES')),lr=LanguageEnum[toLang.upper()].value)
        for num,url in enumerate(articleList):
            #url = content["link"]
            # print(num,url)    
            try:
                articleContent = self.dataFetch(queryCloudEvent,url,toLang.upper())
                # print(articleContent)
                # translate the article content data 
                # translatedArticleContent = self.languageTranslator(articleContent["content"],fromLang="English",toLang='English')
                elasticSearchPayload = self.dataFormatter(queryCloudEvent,articleContent)
                elasticSearchId = self.dataPush(elasticSearchPayload)
                dataDict = ast.literal_eval(elasticSearchPayload["data"])
                dataDict["documentId"] = elasticSearchId
                elasticSearchPayload["data"] = dataDict
                # print(json.dumps(elasticSearchPayload,indent=4))
                payLoadList.append(elasticSearchPayload)
            except SyntaxError as err:
                logger.exception(f'Error pulling articles contents {err}')
            # print("Paylist length",len(payLoadList))
        return payLoadList