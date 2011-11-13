package com.seporaitis.crawler.fetch;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import com.google.protobuf.ByteString;
import com.seporaitis.crawler.protobuf.BundleProtos.Document;

public class HttpFetcher {

    protected final HttpClient client;
    
    public HttpFetcher(HttpClient client) {
        this.client = client;
    }
    
    public Document fetch(URI uri) throws IOException, FetcherException, URISyntaxException {
        Document.Builder doc = Document.newBuilder();
        
        doc.setId(0xDEADBEEF)
           .setDomain(uri.getHost())
           .setUri(uri.toString())
           .setFetchedAt(System.currentTimeMillis())
           .setRank(10);
            
        try {
            HttpGet method = new HttpGet(uri);
            method.setHeader("User-Agent", "jSpider");
            HttpResponse response = client.execute(method);

            doc.setStatus(response.getStatusLine().toString());
            for(Header header : response.getAllHeaders()) {
                doc.addHeader(Document.HttpHeader.newBuilder()
                        .setName(header.getName())
                        .setValue(header.getValue()));
            }

            doc.setContent(ByteString.copyFrom(EntityUtils.toByteArray(response.getEntity())));

            EntityUtils.consume(response.getEntity());
        } catch(ClientProtocolException e) {
            System.err.println(e.getMessage());
            throw new FetcherException("Could not fetch: " + uri.toString());
        }

        return doc.build();
    }
}
