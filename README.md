# Elasticsearch to Neo4j Sync

## Overview

The `ElasticsearchToNeo4jSync` project is a Python-based tool designed to synchronize data between Elasticsearch and a Neo4j graph database. This script is a key component in a data integration pipeline, transforming Elasticsearch documents into Neo4j nodes and relationships based on configurable parameters.

## Key Features

- **Flexible Configuration**: Supports customizable parameters for Elasticsearch queries and Neo4j data transformations.
- **Parallel Processing**: Utilizes multiprocessing to efficiently handle large datasets.
- **Error Handling**: Comprehensive logging and error management for reliable operation.
- **Type and Property Mapping**: Provides robust mapping of document types and properties between Elasticsearch and Neo4j.

## Components

### Main Script

- **`ElasticsearchToNeo4jSync` Class**: The core class that handles the synchronization process, including Elasticsearch querying and Neo4j data pushing.
  - **`__init__`**: Initializes parameters and configurations.
  - **`processNeo4jParams`**: Processes Neo4j parameters for consistency.
  - **`elasticsearchQueryBuilder`**: Constructs Elasticsearch queries based on input parameters.
  - **`neo4jQueryBuilder`**: Builds Neo4j nodes and relationships from Elasticsearch data.
  - **`buildGraphData`**: Generates the data structure needed for Neo4j.
  - **`extractDocument`**: Extracts documents from Elasticsearch response.
  - **`processDocument`**: Filters documents based on configurable thresholds.
  - **`generateDocumentsParallel`**: Uses multiprocessing to handle large volumes of data.
  - **`startProcess`**: Orchestrates the entire data fetching and pushing process.

### Handlers

- **`Neo4jHandler`**: Handles interaction with the Neo4j database, including data pushing.
- **`ElasticsearchHandler`**: Manages queries and data fetching from Elasticsearch.

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/your-repo/elasticsearch-to-neo4j-sync.git
   cd elasticsearch-to-neo4j-sync
   ```

2. **Create and Activate a Virtual Environment**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install Dependencies**

   Ensure you have `requirements.txt` or similar for dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Variables**

   Set the following environment variables for Elasticsearch and Neo4j:

   ```bash
   export ES_HOSTS='your_elasticsearch_hosts'
   export ES_USERNAME='your_elasticsearch_username'
   export ES_PASSWORD='your_elasticsearch_password'
   export ES_CA_CERTS='path_to_ca_certs'
   export ES_CA_FINGERPRINT='your_ca_fingerprint'
   export ES_INDEX='your_elasticsearch_index'

   export NEO4J_HOST='your_neo4j_host'
   export NEO4J_USER='your_neo4j_user'
   export NEO4J_PASSWORD='your_neo4j_password'
   ```

## Usage

1. **Prepare Your Query Cloud Event**

   Construct a dictionary that represents your query parameters for Elasticsearch.

   ```python
   queryCloudEvent = {
       "searchQueries": [
           {
               "properties": [{"subject": "vendor", "value": "example"}]
           }
       ]
   }
   ```

2. **Run the Synchronization**

   ```python
   from your_module import ElasticsearchToNeo4jSync

   sync = ElasticsearchToNeo4jSync()
   response = sync.startProcess(queryCloudEvent)
   print(response)
   ```

## Testing

1. **Unit Tests**

   Ensure you have tests implemented, and run them with:

   ```bash
   pytest
   ```

2. **Integration Tests**

   Test the integration with actual Elasticsearch and Neo4j instances, making sure to validate end-to-end functionality.

## Contributing

Contributions are welcome! Please follow these steps:

1. **Fork the Repository**

2. **Create a Branch**

   ```bash
   git checkout -b feature/your-feature
   ```

3. **Commit Your Changes**

   ```bash
   git commit -am 'Add some feature'
   ```

4. **Push to the Branch**

   ```bash
   git push origin feature/your-feature
   ```

5. **Create a Pull Request**

   Describe the changes you’ve made and why they’re beneficial.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- **Elasticsearch** and **Neo4j** for their powerful data storage and querying capabilities.
- The open-source community for their continuous improvements and support.

---

Feel free to customize the README as needed to fit the specifics of your project and its components.
