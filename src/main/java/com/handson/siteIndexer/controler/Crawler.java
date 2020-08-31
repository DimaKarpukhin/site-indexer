package com.handson.siteIndexer.controler;

import com.handson.siteIndexer.json.*;
import com.handson.siteIndexer.util.ElasticsearchUtil;
import com.handson.siteIndexer.util.KafkaHelper;
import com.squareup.okhttp.*;
import net.minidev.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class Crawler {
    private static final int MAX_MINUTES = 2;
    private static final int MAX_DISTANCE_LIMIT = 10;
    private static final int MAX_TIME_LIMIT = 90000;
    private static final int EMPTY_QUEUE_TIME_LIMIT = 10000;

    @Autowired
    private ElasticsearchUtil elasticsearch;
    @Autowired
    private KafkaHelper kafka;

    private static Set<String> visitedUrls = new HashSet<>();
    private static HashMap<String, CrawlStatus> crawlsCollection = new HashMap<>();

    String searchWithElastic(String crawlId, String text) throws IOException {
        System.out.println(">> receiving data from elastic search: search text->" + text);
        String res = elasticsearch.search(crawlId, text);

        return res.substring(res.indexOf("\"hits\":"));
    }

    CrawlStatus getStatus(String crawlId) {
        return crawlsCollection.get(crawlId);
    }

    String crawl(String baseUrl) {
        String crawlId = UUID.randomUUID().toString();
        crawlsCollection.put(crawlId, new CrawlStatus(
                baseUrl,
                State.RUNNING,
                System.currentTimeMillis(),
                0,
                FinishReason.NOT_FINISHED));
        sendSingleQueueRecordToKafka(crawlId, baseUrl, 0);

        return "{\r\n" +
                "  \"id\": \"" + crawlId +
                "\"\r\n" +
                "}";
    }

    @PostConstruct
    public void startListeningToKafka(){
        Thread thread = new Thread(this::run);
        thread.start();
    }

    private void run() {
        System.out.println(">> Kafka thread running <<");
        long runningTime = 0;
        long startTime = System.currentTimeMillis();
        while (TimeUnit.MILLISECONDS.toMinutes(runningTime) < MAX_MINUTES) {
            List<CrawlerQueueRecord> queueRecords = kafka.recieve(CrawlerQueueRecord.class);
            System.out.println(">> receiving queueRecords from kafka: amount->" + queueRecords.size());
            for (CrawlerQueueRecord queueRecord : queueRecords) {
                crawlSingleUrl(queueRecord);
            }

            checkCrawlsTimeLimits();
            runningTime = System.currentTimeMillis() - startTime;
        }
        System.out.println(">> Kafka stop running <<");
    }

    private void crawlSingleUrl(CrawlerQueueRecord queueRecord) {
        String crawlId = queueRecord.getCrawlId();
        String webPageUrl = queueRecord.getUrl();
        int crawlDistance = queueRecord.getDistance() + 1;

        crawlsCollection.get(crawlId).setStartEmptyTime(System.currentTimeMillis());
        if(crawlDistance >= MAX_DISTANCE_LIMIT){
            updateCrawlStatus(crawlId, FinishReason.MAX_DISTANCE);
        } else {
            process(crawlId, webPageUrl, crawlDistance);
        }
    }

    private void process(String crawlId, String webPageUrl, int crawlDistance) {
        String baseUrl = crawlsCollection.get(crawlId).getBaseUrl();
        Document webPageContent = getWebPageContent(webPageUrl);
        System.out.println(">> extracting urls from current webPage: webPageUrl is " + webPageUrl + " baseUrl is " + baseUrl);
        List<String> innerUrls = extractWebPageUrls(baseUrl, webPageContent);
        addUrlsToQueue(crawlId, innerUrls, crawlDistance);
        addElasticSearch(crawlId, baseUrl, webPageUrl, webPageContent, crawlDistance);
    }

    private void checkCrawlsTimeLimits() {
        for (Map.Entry<String, CrawlStatus> crawlStatus : crawlsCollection.entrySet()) {
            if (crawlStatus.getValue().getState() == State.RUNNING) {
                String crawlId = crawlStatus.getKey();
                CrawlStatus status = crawlStatus.getValue();
                if (System.currentTimeMillis() - status.getStartEmptyTime() > EMPTY_QUEUE_TIME_LIMIT)  {
                    updateCrawlStatus(crawlId, FinishReason.EMPTY_QUEUE);
                }
                if (System.currentTimeMillis() - status.getStartTime() > MAX_TIME_LIMIT)  {
                    updateCrawlStatus(crawlId, FinishReason.TIMEOUT);
                }
            }
        }
    }

    private void updateCrawlStatus(String crawlId, FinishReason finishReason) {
        if(crawlsCollection.get(crawlId).getState() == State.RUNNING) {
            crawlsCollection.get(crawlId).setState(State.FINISHED);
            crawlsCollection.get(crawlId).setFinishReason(finishReason);
        }
    }

    private void addUrlsToQueue(String crawlId, List<String> urls, int distance) {
        System.out.println(">> adding urls to queue: distance->" + distance + " amount->" + urls.size());
        crawlsCollection.get(crawlId).setDistanceFromRoot(distance);
        for (String url : urls) {
            if (!visitedUrls.contains(crawlId + url)) {
                visitedUrls.add(crawlId + url);
                sendSingleQueueRecordToKafka(crawlId, url, distance);
            }
        }
    }

    private void sendSingleQueueRecordToKafka(String crawlId, String url, int distance){
        System.out.println(">> sending url to kafka: distance->" + distance + "\n   url->" + url);
        CrawlerQueueRecord queueRecord = new CrawlerQueueRecord();
        queueRecord.setCrawlId(crawlId);
        queueRecord.setUrl(url);
        queueRecord.setDistance(distance);
        kafka.send(queueRecord);
    }

    private void addElasticSearch(String crawlId, String baseUrl, String webPageUrl,
                                  Document webPageContent, int level) {
        System.out.println(">> adding elastic search for webPage: " + baseUrl);
        String text = String.join(" ", webPageContent.select("a[href]").eachText());
        UrlSearchDoc searchDoc = UrlSearchDoc.of(crawlId, text, webPageUrl, baseUrl, level);
        elasticsearch.addData(searchDoc);
    }

    private List<String> extractWebPageUrls(String baseUrl, Document webPageContent) {
        List<String> links = webPageContent.select("a[href]").eachAttr("abs:href");

        return links.stream().filter(url -> url.startsWith(baseUrl)).collect(Collectors.toList());
    }

    private Document getWebPageContent(String webPageUrl) {
        String content = getHttp(webPageUrl);
        return Jsoup.parse(content);
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

//    @PostConstruct
//    void readFirstRecordFromKafka() {
//        kafka.recieve(CrawlerQueueRecord.class);
//    }
//    private String getText(String webPageUrl) {
//        System.out.println(">>getting text from webPage: " + webPageUrl);
//        Elements links = extractLinksFromWebPage(webPageUrl);
//
//        return String.join("\n", links.eachText());
//    }

//    private Elements extractLinksFromWebPage(String webPageUrl){
//        System.out.println(">>extracting links from webPage: " + webPageUrl);
//        String content = getHttp(webPageUrl);
//        Document doc = Jsoup.parse(content);
//
//        return doc.select("a[href]");
//    }

}
