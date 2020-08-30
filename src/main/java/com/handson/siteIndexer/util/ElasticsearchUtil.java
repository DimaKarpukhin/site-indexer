package com.handson.siteIndexer.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.handson.siteIndexer.json.UrlSearchDoc;
import com.squareup.okhttp.*;
import org.apache.tomcat.util.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class ElasticsearchUtil {

    @Value("${elastic.base.url}")
    private String baseUrl;

    @Autowired
    RestTemplate restTemplate;

    @Autowired
    ObjectMapper om;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchUtil.class);


    public  void addData(UrlSearchDoc doc)  {
        try {
            String auth =  new String(Base64.encodeBase64("site:8c6d4815e5340e273775354f46e86774".getBytes()));
            String content = om.writeValueAsString(doc);
            OkHttpClient client =  new OkHttpClient();
            MediaType mediaType = MediaType.parse("application/json");
            RequestBody body = RequestBody.create(mediaType, content);
            System.out.println(om.writeValueAsString(doc));
            System.out.println(doc.toString());
            Request request = new Request.Builder()
                    .url(baseUrl +  "/doc")
                    .method("POST", body)
                    .addHeader("Content-Type", "application/json")
                    .addHeader(HttpHeaders.AUTHORIZATION, "Basic " + auth)
                    .build();
            Response response =  client.newCall(request).execute();
            System.out.println("@@@@" + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  Response search()  {
        Response response = null;
        try {
            OkHttpClient client = new OkHttpClient();
            Request request = new Request.Builder()
                    .url("https://site:8c6d4815e5340e273775354f46e86774@gimli-eu-west-1.searchly.com/elastic/_search" )
                    .method("GET", null)
                    .build();
            response = client.newCall(request).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return response;
    }
}