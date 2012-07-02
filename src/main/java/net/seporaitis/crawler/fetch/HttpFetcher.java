/**
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
package net.seporaitis.crawler.fetch;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import com.google.protobuf.ByteString;

import net.seporaitis.crawler.protobuf.BundleProtos;


/**
 * Fetches the given URI and returns Document that
 * can be added to Bundle.
 */
public class HttpFetcher {

    private final Log logger = LogFactory.getLog(HttpFetcher.class);

    protected final HttpClient client;

    public HttpFetcher(HttpClient client) {
        this.client = client;
    }

    /**
     * Fetch the given URI.
     *
     * @param URI uri
     *
     * @return BundleProtos.Document
     *
     * @throws IOException
     * @throws FetcherException
     * @throws URISyntaxException
     */
    public BundleProtos.Document fetch(URI uri) throws IOException,
                                                       FetcherException,
                                                       URISyntaxException {
        BundleProtos.Document.Builder doc = BundleProtos.Document.newBuilder();

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
            for (Header header : response.getAllHeaders()) {
                doc.addHeader(BundleProtos.Document.HttpHeader.newBuilder()
                              .setName(header.getName())
                              .setValue(header.getValue()));
            }

            doc.setContent(ByteString.copyFrom(EntityUtils.toByteArray(response.getEntity())));

            EntityUtils.consume(response.getEntity());
        } catch (ClientProtocolException e) {
            logger.error("Could not fetch: '" + uri.toString() + "'", e);
            throw new FetcherException("Could not fetch: " + uri.toString());
        }

        return doc.build();
    }

}
