# NOTE: NEEDS REWRITE FOR DATA-INGRESS-${SOURCE} STRUCTURE
# PSI Gray Hat Warfare Scraper
Downloads files in openbuckets via matching a query to the titles of some file within GrayHatHacker's Bucket API. Uses tika via pypi package python-tika to identify the file types and metadata pertaining to it and pushes the relevant data to an index specified by the user. 

## To use Gray Hat Warfare Scraper Requires 3 steps.

### :bar_chart: _Querying the data_
Gray Hat Warfare scraper accepts two flags:
* -q / --query 
* -e / --extensions

### :nut_and_bolt: _Docker Build Instructions_
```sh
docker build -t grayhatwarfare_scraper
```
### :wrench: _Configuration_
Please ensure GrayHatWarfareScraper is properly configured to your elastic search instance. For more information please see (elasticsearch's webpage)[https://www.elastic.co/guide/en/cloud/current/ec-cloud-id.html#:~:text=Log%20in%20to%20the%20Elasticsearch,copy%20down%20the%20Cloud%20ID.]

put more here.

### :runner: _Running the container_
```sh
docker run --network ${ELASTIC_NETWORK} grayhatwarfare_scraper --query ${QUERY} --extensions ${EXTENSIONS}
```