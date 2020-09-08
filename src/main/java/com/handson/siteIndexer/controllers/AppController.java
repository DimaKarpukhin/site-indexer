package com.handson.siteIndexer.controllers;

import com.handson.siteIndexer.entities.CrawlStatus;
import com.handson.siteIndexer.utils.KafkaUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

/**
 * Spring Boot Hello案例
 * <p>
 * Created by bysocket on 26/09/2017.
 */
@RestController
@RequestMapping(value = "")
public class AppController
{
    @Autowired
    KafkaUtil kafka;

    @Autowired
    CrawlController crawler;

    @RequestMapping(value = "/kafkaListener", method = RequestMethod.GET)
    public void invokeKafkaListener() {
        crawler.startListeningToKafka();
    }

    @RequestMapping(value = "/crawler", method = RequestMethod.GET)
    public String crawl(String baseUrl) {
        return crawler.crawl(baseUrl);
    }

    @RequestMapping(value = "/crawlStatus", method = RequestMethod.GET)
    public CrawlStatus getCrawlStatus(String crawlId) {
        return crawler.getStatus(crawlId);
    }

    @RequestMapping(value = "/elasticSearch", method = RequestMethod.POST)
    public String searchWithElastic(String crawlId, String text) throws IOException {
        return crawler.searchWithElastic(crawlId, text);
    }
}