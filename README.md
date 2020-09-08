# site-indexer
Server rest api for crawling over the links of provided url (DFS algorithm). Implemented with kafka database and elastic search.

Test api on: https://dima-site-indexer.herokuapp.com/swagger-ui.html

----------------
###  API provides:
* **GET(_method_: invokeKafkaListener) -** Makes kafka DB ready to send/receive the data.
* **GET(_method_: crawl) -** Starts crawling over the links of provided url(DFS). Returns crawl ID for checking crawl status. Save the data to kafka DB and elastic search.
* **GET(_method_: getCrawlStatus) -** Provides information about crawling process according to crawl ID.
* **POST(_method_: searchWithElastic) -** Finds text all the links of specific crawling process according to crawl ID and keywords.
