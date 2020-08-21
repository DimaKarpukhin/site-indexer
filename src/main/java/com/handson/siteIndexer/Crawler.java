package com.handson.siteIndexer;

import com.handson.siteIndexer.json.CrawlerQueueRecord;
import com.handson.siteIndexer.json.UrlSearchDoc;
import com.handson.siteIndexer.util.ElasticsearchUtil;
import com.handson.siteIndexer.util.KafkaHelper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class Crawler {
    private static final int MAX_DISTANCE = 7;
    private static final int EMPTY_QUEUE_TIME_LIMIT = 5;
    private static final int MAX_TIME_LIMIT = 5;

    @Autowired
    ElasticsearchUtil elasticsearch;

    @Autowired
    KafkaHelper kafka;
    private Set<String> visited = new HashSet<>();

    public String crawl(String baseUrl) {
        String result = "finished";
        visited.clear();
        List<String> urls = getUrls(baseUrl).stream().filter(url -> url.startsWith(baseUrl)).collect(Collectors.toList());
        addUrlsToQueue(urls, 1);
        addElasticSearch(baseUrl, baseUrl, 0);
        List<CrawlerQueueRecord> queueRecords;
        int currentDistance = 0;
        long emptyQueueStartTime = 0;
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < MAX_TIME_LIMIT && currentDistance < MAX_DISTANCE) {
            queueRecords = kafka.recieve(CrawlerQueueRecord.class);
            if(queueRecords.isEmpty()){
                emptyQueueStartTime = emptyQueueStartTime == 0 ? System.currentTimeMillis() : emptyQueueStartTime;
                if(System.currentTimeMillis() - emptyQueueStartTime > EMPTY_QUEUE_TIME_LIMIT) { break; }
            }

            int i = 0;
            currentDistance = !queueRecords.isEmpty() && queueRecords.get(i) != null ? queueRecords.get(i).getDistance(): currentDistance;
            while (i < queueRecords.size() && currentDistance < MAX_DISTANCE) {
                CrawlerQueueRecord queueRecord = queueRecords.get(i);
                currentDistance = queueRecord.getDistance();
                urls = getUrls(queueRecord.getUrl()).stream().filter(url -> url.startsWith(baseUrl)).collect(Collectors.toList());
                addElasticSearch(queueRecord.getUrl(), baseUrl, currentDistance + 1);
                addUrlsToQueue(urls, currentDistance + 1);
                i++;
            }
        }

        return result;
    }

    private void addUrlsToQueue(List<String> buffer, int distance)
    {
        for (String url : buffer) {
            if (!visited.contains(url)) {
                visited.add(url);
                sendSingleQueueRecordToKafka(url, distance);
            }
        }
    }

    private void sendSingleQueueRecordToKafka(String url, int distance){
        CrawlerQueueRecord queueRecord = new CrawlerQueueRecord();
        queueRecord.setUrl(url);
        queueRecord.setDistance(distance);
        kafka.send(queueRecord);
    }

    private void addElasticSearch(String url, String baseUrl, int level) {
        String text = getText(url);
        UrlSearchDoc searchDoc = UrlSearchDoc.of(text, url, baseUrl, level);
        elasticsearch.addData(searchDoc);
    }

    private List<String> getUrls(String webPageUrl) {
        System.out.println(">>getting urls from webPage: " + webPageUrl);
        Elements links = extractLinksFromWebPage(webPageUrl);

        return links.eachAttr("abs:href");
    }

    private String getText(String webPageUrl) {
        System.out.println(">>getting text from webPage: " + webPageUrl);
        Elements links = extractLinksFromWebPage(webPageUrl);

        return String.join("\n", links.eachText());
    }

    private Elements extractLinksFromWebPage(String webPageUrl){
        System.out.println(">>extracting links from webPage: " + webPageUrl);
        String content = getHttp(webPageUrl);
        Document doc = Jsoup.parse(content);

        return doc.select("a[href]");
    }

    private String getHttp(String requestURL){
        if (requestURL == null || requestURL.trim().equals("")){
            return "";
        }
        try (Scanner scanner = new Scanner(new URL(requestURL).openStream(),
                StandardCharsets.UTF_8.toString())) {
            scanner.useDelimiter("\\A");

            return scanner.hasNext() ? scanner.next() : "";
        }
        catch (Exception e) {
            return "";
        }
    }

    @PostConstruct
    void readFirstRecordFromKafka() {
        kafka.recieve(CrawlerQueueRecord.class);
    }
}
