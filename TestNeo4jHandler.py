import unittest
from unittest.mock import MagicMock, patch
from logging import Logger
from Neo4jHandler import Neo4jHandler


class TestNeo4jHandler(unittest.TestCase):

    def setUp(self):
        self.params = {
            "nodeTypes": ["Person", "Movie"],
            "relationTypes": ["ACTED_IN", "DIRECTED"]
        }
        self.uri = "bolt://localhost:7687"
        self.user = "neo4j"
        self.password = "password"
        self.logger = Logger("TestNeo4jHandler")
        self.neo4j_handler = Neo4jHandler(self.params, self.uri, self.user, self.password, self.logger)

    @patch('Neo4jHandler.GraphDatabase')
    def test_formatProps_with_props(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver

        props = {"name": "Tom Hanks", "birthyear": 1956}
        expected_output = "{name: 'Tom Hanks', birthyear: '1956'}"

        self.assertEqual(self.neo4j_handler.formatProps(props), expected_output)

    @patch('Neo4jHandler.GraphDatabase')
    def test_formatProps_without_props(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver

        props = None
        expected_output = ""

        self.assertEqual(self.neo4j_handler.formatProps(props), expected_output)

    @patch('Neo4jHandler.GraphDatabase')
    def test_createRelationship(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver
        relationshipType = "ACTED_IN"
        relationshipProps = {"roles": ["Forrest Gump"]}
        expected_output = ""
    
    @patch('Neo4jHandler.GraphDatabase')
    def test_createNode(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver

        nodeType = "Person"
        nodeProps = {"name": "Tom Hanks", "birthyear": 1956}
        with self.assertRaises(Exception):
            self.neo4j_handler.createNode(nodeType=nodeType, 
                                          nodeProps=nodeProps)
    @patch('Neo4jHandler.GraphDatabase')
    def test_init(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver
        neo4j_handler = Neo4jHandler(self.params, self.uri, self.user, self.password, self.logger)

        self.assertEqual(neo4j_handler.params, self.params)
        self.assertEqual(neo4j_handler.logger, self.logger)

    def test_formatProps_with_empty_props(self):
        props = {}
        expected_output = ""
        self.assertEqual(self.neo4j_handler.formatProps(props), expected_output)

    def test_formatProps_with_int_props(self):
        props = {"age": 30, "birthyear": 1992}
        expected_output = "{age: '30', birthyear: '1992'}"
        self.assertEqual(self.neo4j_handler.formatProps(props), expected_output)

    def test_createRelationship_without_props(self):
        relationshipType = "ACTED_IN"
        relationshipProps = None
        with self.assertRaises(Exception):
            self.neo4j_handler.createRelationship(None, None, relationshipType, relationshipProps, None, None)

    def test_createNode_without_props(self):
        nodeType = "Person"
        nodeProps = None
        with self.assertRaises(Exception):
            self.neo4j_handler.createRelationship(fromNodeType=nodeType, 
                                                  fromNodeProps=nodeProps, 
                                                  relationshipType=None, 
                                                  relationshipProps=None, 
                                                  toNodeType=None,
                                                  toNodeProps=None)

    def test_createDyad_without_relationship_props(self):
        from_nodeType = "Person"
        from_nodeProps = {"name": "Tom Hanks", "birthyear": 1956}
        relationship_type = "ACTED_IN"
        relationship_props = None
        to_nodeType = "Movie"
        to_nodeProps = {"title": "Forrest Gump", "released": 1994}
        with self.assertRaises(Exception):
            self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)

    def test_createDyad_without_from_node_props(self):
            from_nodeType = "Person"
            from_nodeProps = None
            relationship_type = "ACTED_IN"
            relationship_props = {"roles": ["Forrest Gump"]}
            to_nodeType = "Movie"
            to_nodeProps = {"title": "Forrest Gump", "released": 1994}
            with self.assertRaises(Exception):
                self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)


    def test_createDyad_without_to_node_props(self):
        from_nodeType = "Person"
        from_nodeProps = {"name": "Tom Hanks", "birthyear": 1956}
        relationship_type = "ACTED_IN"
        relationship_props = {"roles": ["Forrest Gump"]}
        to_nodeType = "Movie"
        to_nodeProps = None
        with self.assertRaises(TypeError):
            self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)

    @patch('Neo4jHandler.GraphDatabase')
    def test_init_without_neo4jParameters(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver
        uri = None
        neo4j_handler = Neo4jHandler(neo4jParameters=self.params, 
                                     uri=uri, 
                                     user=self.user,
                                     password=self.password, 
                                     logger=self.logger)
        mock_graph_db.driver.assert_called_once_with(uri=None, 
                                                     auth=(self.user, self.password))
        self.assertEqual(neo4j_handler.params, self.params)
        
    @patch('Neo4jHandler.GraphDatabase')
    def test_init_without_uri(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver
        uri = None
        neo4j_handler = Neo4jHandler(neo4jParameters=self.params, 
                                     uri=uri, 
                                     user=self.user,
                                     password=self.password, 
                                     logger=self.logger)
        mock_graph_db.driver.assert_called_once_with(uri=None, 
                                                     auth=(self.user, self.password))
        self.assertEqual(neo4j_handler.params, self.params)

    @patch('Neo4jHandler.GraphDatabase')
    def test_init_without_user(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver
        user = None
        neo4j_handler = Neo4jHandler(neo4jParameters=self.params, 
                                     uri=self.uri, 
                                     user=user,
                                     password=self.password, 
                                     logger=self.logger)
        mock_graph_db.driver.assert_called_once_with(uri=self.uri, 
                                                     auth=(user, self.password))
        self.assertEqual(neo4j_handler.params, self.params)


    @patch('Neo4jHandler.GraphDatabase')
    def test_init_without_password(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver
        password = None
        neo4j_handler = Neo4jHandler(neo4jParameters=self.params, 
                                     uri=self.uri, 
                                     user=self.user,
                                     password=password, 
                                     logger=self.logger)
        mock_graph_db.driver.assert_called_once_with(uri=self.uri, auth=(self.user, password))
        self.assertEqual(neo4j_handler.params, self.params)

    @patch('Neo4jHandler.GraphDatabase')
    def test_init_without_logger(self, mock_graph_db):
        mock_driver = MagicMock()
        mock_graph_db.driver.return_value = mock_driver
        logger = None
        neo4j_handler = Neo4jHandler(neo4jParameters=self.params, 
                                     uri=self.uri, 
                                     user=self.user,
                                     password=self.password, 
                                     logger=logger)
        mock_graph_db.driver.assert_called_once_with(uri=self.uri, auth=(self.user, self.password))
        self.assertEqual(neo4j_handler.params, self.params)

    def test_createNode_invalid_type(self):
        nodeType = "INVALID_TYPE"
        nodeProps = {"name": "Tom Hanks", "birthyear": 1956}
        with self.assertRaises(Exception):
            self.neo4j_handler.createNode(nodeType, nodeProps)

    def test_createDyad_invalid_node_type(self):
        from_nodeType = "Person"
        from_nodeProps = {"name": "Tom Hanks", "birthyear": 1956}
        relationship_type = "ACTED_IN"
        relationship_props = {"roles": ["Forrest Gump"]}
        to_nodeType = "INVALID_TYPE"
        to_nodeProps = {"title": "Forrest Gump", "released": 1994}
        with self.assertRaises(Exception):
            self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)

    def test_createDyad_invalid_relationship_type(self):
        from_nodeType = "Person"
        from_nodeProps = {"name": "Tom Hanks", "birthyear": 1956}
        relationship_type = "INVALID_TYPE"
        relationship_props = {"roles": ["Forrest Gump"]}
        to_nodeType = "Movie"
        to_nodeProps = {"title": "Forrest Gump", "released": 1994}
        with self.assertRaises(Exception):
            self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)

    def test_createDyad_without_to_node_props(self):
        from_nodeType = "Person"
        from_nodeProps = {"name": "Tom Hanks", "birthyear": 1956}
        relationship_type = "ACTED_IN"
        relationship_props = {"roles": ["Forrest Gump"]}
        to_nodeType = "Movie"
        to_nodeProps = None
        with self.assertRaises(Exception):
            self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)

    # def test_create_node_with_empty_node_props(self):
    #     with self.assertRaises(ValueError):
    #         self.neo4j_handler.createNode("Person", {})


    # def test_create_relationship_with_empty_relationship_props(self):
    #     result = self.neo4j_handler.createDyad(from_nodeType="Person", relationshipType="KNOWS", to_nodeType="Person")
    #     with self.assertRaises(ValueError):
    #         self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)

    # def test_create_dyad_with_empty_from_node_props(self):
    #     with self.assertRaises(ValueError):
    #         self.neo4j_handler.createDyad(from_nodeType, from_nodeProps, relationship_type, relationship_props, to_nodeType, to_nodeProps)

    # def test_create_dyad_with_empty_relationship_props(self):
    #     result = self.neo4j_handler.createDyad("Person", {"name": "Alice"}, "KNOWS", "Person", {"name": "Bob"}, {})
    #     self.assertIsNotNone(result)

    # def test_create_dyad_with_empty_to_node_props(self):
    #     result = self.neo4j_handler.createDyad("Person", {"name": "Alice"}, "KNOWS", "Person", {}, {})
    #     self.assertIsNotNone(result)

    # def test_create_dyad_with_nonexistent_from_node_type(self):
    #     with self.assertRaises(Exception):
    #         self.neo4j_handler.createDyad("NonexistentNodeType", {"name": "Alice"}, "KNOWS", "Person", {"name": "Bob"}, {"since": "2020"})

    # def test_create_dyad_with_nonexistent_to_node_type(self):
    #     with self.assertRaises(Exception):
    #         self.neo4j_handler.createDyad("Person", {"name": "Alice"}, "KNOWS", "NonexistentNodeType", {"name": "Bob"}, {"since": "2020"})

    # def test_create_dyad_with_nonexistent_relationship_type(self):
    #     with self.assertRaises(Exception):
    #         self.neo4j_handler.createDyad("Person", {"name": "Alice"}, "NonexistentRelationshipType", "Person", {"name": "Bob"}, {"since": "2020"})

    # def test_create_dyad_with_disallowed_types(self):
    #     with self.assertRaises(Exception):
    #         self.neo4j_handler.createDyad("Person", {"name": "Alice"}, "LOVES", "Person", {"name": "Bob"}, {"since": "2020"})

    # def test_create_dyad_with_special_characters_in_relationship_props(self):
    #     with self.assertRaises(Exception):
    #         self.neo4j_handler.createDyad("Person", {"name": "Alice"}, "KNOWS", "Person", {"name": "Bob"}, {"@since": "2020"})
    
    def close(self):
        self.driver.close()
        
if __name__ == '__main__':
    unittest.main()