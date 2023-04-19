import unittest
from unittest.mock import Mock
from elasticsearch import Elasticsearch
from logging import Logger
from .ElasticsearchHandler import ElasticsearchHandler

class TestElasticsearchHandler(unittest.TestCase):
    def setUp(self):
        self.logger = Mock(spec=Logger)
        self.handler = ElasticsearchHandler('localhost:9200', 'testuser', 'testpass', 'test_certs.pem', 'test_fingerprint', 'test_index', self.logger)
        
    def test_dataFetch(self):
        # Case 1: When search results are returned
        query = {'query': {'match_all': {}}}
        expected_response = {'_scroll_id': 'fake_scroll_id', 'took': 1, 'timed_out': False, '_shards': {'total': 1, 'successful': 1, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 1, 'relation': 'eq'}, 'max_score': 1.0, 'hits': [{'_index': 'test_index', '_type': '_doc', '_id': '123', '_score': 1.0, '_source': {'field1': 'value1', 'field2': 'value2'}}]}}
        self.handler.client.search = Mock(return_value=expected_response)
        actual_response = self.handler.dataFetch(query)
        self.assertEqual(actual_response, expected_response)
        self.handler.client.search.assert_called_once_with(index='test_index', query=query)

        # Case 2: When no search results are returned
        query = {'query': {'match_none': {}}}
        expected_response = None
        self.handler.client.search = Mock(return_value=expected_response)
        actual_response = self.handler.dataFetch(query)
        self.assertEqual(actual_response, expected_response)
        self.handler.client.search.assert_called_once_with(index='test_index', query=query)

    def test_init(self):
        # Case 1: When all required parameters are provided
        Elasticsearch = Mock(spec=Elasticsearch)
        handler = ElasticsearchHandler('localhost:9200', 'testuser', 'testpass', 'test_certs.pem', 'test_fingerprint', 'test_index', self.logger)
        self.assertEqual(handler.index, 'test_index')
        self.assertEqual(handler.logger, self.logger)
        Elasticsearch.assert_called_once_with(
            hosts=['localhost:9200'],
            http_auth=('testuser', 'testpass'),
            ca_certs='test_certs.pem',
            ssl_assert_fingerprint='test_fingerprint',
            verify_certs=True
        )

        # Case 2: When some optional parameters are not provided
        Elasticsearch = Mock(spec=Elasticsearch)
        handler = ElasticsearchHandler('localhost:9200', 'testuser', 'testpass', None, None, 'test_index', self.logger)
        self.assertEqual(handler.index, 'test_index')
        self.assertEqual(handler.logger, self.logger)
        Elasticsearch.assert_called_once_with(
            hosts=['localhost:9200'],
            http_auth=('testuser', 'testpass'),
            ca_certs=None,
            ssl_assert_fingerprint=None,
            verify_certs=False
        )

        # Case 3: When the Elasticsearch client fails to connect
        Elasticsearch = Mock(side_effect=Exception('failed to connect'))
        logger = Mock(spec=Logger)
        handler = ElasticsearchHandler('localhost:9200', 'testuser', 'testpass', 'test_certs.pem', 'test_fingerprint', 'test_index', logger)
        self.assertIsNone(handler.client)
        logger.error.assert_called_once_with('Failed to connect to Elasticsearch: failed to connect')

if __name__ == '__main__':
    unittest.main()