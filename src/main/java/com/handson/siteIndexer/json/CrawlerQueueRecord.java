package com.handson.siteIndexer.json;

import java.util.Objects;

public class CrawlerQueueRecord {
    private String url;
    private int distance;
    private String crawlId = "";

    public static CrawlerQueueRecord of (String url, int distance) {
        CrawlerQueueRecord res = new CrawlerQueueRecord();
        res.url = url;
        res.distance = distance;
        return res;
    }

    @Override
    public String toString() {
        return "CrawlerQueueRecord{" +
                "url='" + url + '\'' +
                ", distance=" + distance +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CrawlerQueueRecord that = (CrawlerQueueRecord) o;
        return distance == that.distance &&
                Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, distance);
    }

    public String getCrawlId() {
        return crawlId;
    }

    public void setCrawlId(String crawlId) {
        this.crawlId = crawlId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }
}
