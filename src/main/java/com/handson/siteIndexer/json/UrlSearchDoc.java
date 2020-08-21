package com.handson.siteIndexer.json;

import java.util.Objects;

public class UrlSearchDoc {
    String url;
    String baseUrl;
    String content;
    int level;

    public static UrlSearchDoc of(String content, String url, String baseUrl, int level) {
        UrlSearchDoc res = new UrlSearchDoc();
        res.url = url;
        res.baseUrl = baseUrl;
        res.content = content;
        res.level = level;
        return res;
    }

    @Override
    public String toString() {
        return "UrlSearchDoc{" +
                "url='" + url + '\'' +
                ", baseUrl='" + baseUrl + '\'' +
                ", content='" + content + '\'' +
                ", level=" + level +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UrlSearchDoc that = (UrlSearchDoc) o;
        return level == that.level &&
                Objects.equals(url, that.url) &&
                Objects.equals(baseUrl, that.baseUrl) &&
                Objects.equals(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hash(url, baseUrl, content, level);
    }

    public String getUrl() {
        return url;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public String getContent() {
        return content;
    }

    public int getLevel() {
        return level;
    }
}
