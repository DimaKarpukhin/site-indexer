package com.handson.siteIndexer.entities;

public class CrawlStatus {
    private String baseUrl;
    private State state;
    private long startTime;
    private long startEmptyTime;
    private int distanceFromRoot;
    private FinishReason finishReason;

    public CrawlStatus() { }

    public CrawlStatus(String baseUrl, State state, long startTime, int distanceFromRoot, FinishReason finishReason) {
        this.baseUrl = baseUrl;
        this.state = state;
        this.startTime = startTime;
        this.distanceFromRoot = distanceFromRoot;
        this.finishReason = finishReason;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartEmptyTime() {
        return startEmptyTime;
    }

    public void setStartEmptyTime(long startEmptyTime) {
        this.startEmptyTime = startEmptyTime;
    }


    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public int getDistanceFromRoot() {
        return distanceFromRoot;
    }

    public void setDistanceFromRoot(int distanceFromRoot) {
        this.distanceFromRoot = distanceFromRoot;
    }

    public FinishReason getFinishReason() {
        return finishReason;
    }

    public void setFinishReason(FinishReason finishReason) {
        this.finishReason = finishReason;
    }

    @Override
    public String toString() {
        return "CrawlStatus{" +
                "state=" + state +
                ", distanceFromRoot=" + distanceFromRoot +
                ", finishReason=" + finishReason +
                '}';
    }
}
