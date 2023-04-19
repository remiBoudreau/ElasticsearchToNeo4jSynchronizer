import unittest
from unittest.mock import patch, Mock
from elasticsearch import Elasticsearch
from logging import Logger
from typing import List, Dict, Union
from ElasticsearchHandler import ElasticsearchHandler


class TestElasticsearchHandler(unittest.TestCase):
    def setUp(self):
        self.hosts = ['https://localhost:9200']
        self.username = 'username'
        self.password = 'password'
        self.caCerts = 'ca_certs'
        self.caFingerprint = ''.join(['a']*32)
        self.index = 'test_index'
        self.logger = Logger('test_logger')
        
    @patch.object(Elasticsearch, 'search')
    def test_data_fetch(self, mock_search):
        # create an instance of ElasticsearchHandler
        es_handler = ElasticsearchHandler(
            hosts=self.hosts,
            username=self.username,
            password=self.password,
            caCerts=self.caCerts,
            caFingerprint=self.caFingerprint,
            index=self.index,
            logger=self.logger
        )

        # test successful data fetch
        mock_response = {'hits': {'total': {'value': 1}, 'hits': [{'_id': '1', '_source': {}}]}}
        mock_search.return_value = mock_response
        query = {'query': {'match_all': {}}}
        result = es_handler.dataFetch(query)
        self.assertEqual(result, mock_response)

        # test other exception during data fetch
        mock_search.side_effect = Exception('test error')
        with self.assertRaises(Exception) as context:
            es_handler.dataFetch(query)
        self.assertEqual(str(context.exception), 'Failed to retrieve data from Elasticsearch: test error')
        self.assertTrue(mock_search.called)

        @patch.object(Elasticsearch, 'search')
        def test_constructor(self, mock_search):
            # create an instance of ElasticsearchHandler
            es_handler = ElasticsearchHandler(
                hosts=self.hosts,
                username=self.username,
                password=self.password,
                caCerts=self.caCerts,
                caFingerprint=self.caFingerprint,
                index=self.index,
                logger=self.logger
            )

            # test Elasticsearch object creation with default values
            self.assertIsInstance(es_handler.client, Elasticsearch)
            self.assertEqual(es_handler.client.transport.hosts, self.hosts)
            self.assertEqual(es_handler.client.transport.http_auth, (self.username, self.password))
            self.assertEqual(es_handler.client.transport.use_ssl, True)
            self.assertEqual(es_handler.client.transport.verify_certs, True)
            self.assertEqual(es_handler.client.transport.ssl.ca_certs, self.caCerts)
            self.assertEqual(es_handler.client.transport.ssl.ca_certs_digests, [self.caFingerprint])

            # test Elasticsearch object creation with custom values
            es_handler = ElasticsearchHandler(
                hosts=self.hosts,
                username=self.username,
                password=self.password,
                useSSL=False,
                verifyCerts=False,
                index=self.index,
                logger=self.logger
            )
            self.assertIsInstance(es_handler.client, Elasticsearch)
            self.assertEqual(es_handler.client.transport.hosts, self.hosts)
            self.assertEqual(es_handler.client.transport.http_auth, (self.username, self.password))
            self.assertEqual(es_handler.client.transport.use_ssl, False)
            self.assertEqual(es_handler.client.transport.verify_certs, False)
            self.assertIsNone(es_handler.client.transport.ssl.ca_certs)
            self.assertIsNone(es_handler.client.transport.ssl.ca_certs_digests)

            self.assertTrue(mock_search.called)


        # test exception during connection
        with patch.object(Elasticsearch, '__init__', side_effect=Exception('test error')):
            es_handler = ElasticsearchHandler(
                hosts=self.hosts,
                username=self.username,
                password=self.password,
                caCerts=self.caCerts,
                caFingerprint=self.caFingerprint,
                index=self.index,
                logger=self.logger
            )
        self.assertIsNone(es_handler.client)
        with self.assertRaises(Exception):
            es_handler.dataFetch({})  # attempt to use the uninitialized client instance
            self.assertEqual(result['error'], 'Failed to retrieve data from Elasticsearch: test error')
            self.assertIn('test error', result['error'])
            self.assertTrue(mock_search.called)

if __name__ == '__main__':
    unittest.main()