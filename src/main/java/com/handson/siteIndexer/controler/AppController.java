package com.handson.siteIndexer.controler;

import com.handson.siteIndexer.json.CrawlStatus;
import com.handson.siteIndexer.util.KafkaHelper;
import com.squareup.okhttp.Response;
import net.minidev.json.JSONObject;
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
@RequestMapping(value = "/app")
public class AppController
{
    int i = 0;
    @Autowired
    KafkaHelper kafka;

    @Autowired
    Crawler crawler;

    @RequestMapping(value = "/hello", method = RequestMethod.GET)
    public String sayHello()
    {
        return "Hello!";
    }

    @RequestMapping(value = "/kafka", method = RequestMethod.GET)
    public void invokeKafkaListener() {
        crawler.startListeningToKafka();
    }

    @RequestMapping(value = "/crawl", method = RequestMethod.GET)
    public String crawl(String baseUrl) {
        return crawler.crawl(baseUrl);
    }

    @RequestMapping(value = "/status", method = RequestMethod.GET)
    public CrawlStatus getCrawlStatus(String crawlId) {
        return crawler.getStatus(crawlId);
    }

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public String searchFromElastic(String crawlId, String text) throws IOException {
        return crawler.searchFromElastic(crawlId, text);
    }
}