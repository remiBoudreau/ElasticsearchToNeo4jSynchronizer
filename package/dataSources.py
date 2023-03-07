from enum import Enum
class DataSources(Enum):
    CVE = "CVE"
    DATASCRAPER = "dataScraper"         # scrape articles
    PEOPLEDATALABS = "peopleDataLabs"
    COAUTHORSEARCH = "coAuthors"
    SOCIALMEDIAEXTRACTOR = "socialMediaExtractor"
    EMAILBREACHDETECTOR = "emailBreachDetector"
    SAMSDATASET = "samsDataset"