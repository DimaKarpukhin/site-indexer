package com.handson.siteIndexer;

import com.handson.siteIndexer.util.KafkaHelper;
import kafka.Kafka;
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

    @RequestMapping(value = "/Hello", method = RequestMethod.GET)
    public String SayHello()
    {
        return "Hello!";
    }

    @RequestMapping(value = "/crawl", method = RequestMethod.GET)
    public String crawl(String baseUrl) throws IOException {
        return crawler.crawl(baseUrl);
    }

    @RequestMapping(value = "/KafkaTest", method = RequestMethod.GET)
    public String KafkaTest()
    {
        kafka.send("Hello" + ++i);

        return kafka.recieve(String.class).toString();
    }

}