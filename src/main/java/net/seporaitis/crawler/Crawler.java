/**
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
package net.seporaitis.crawler;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import net.seporaitis.crawler.fetch.HttpFetcher;
import net.seporaitis.crawler.queue.RedisUriQueue;
import net.seporaitis.crawler.protobuf.BundleProtos;


public class Crawler {

    private static final Log logger = LogFactory.getLog(Crawler.class);

    final static int BUNDLE_PAGE_LIMIT = 512;
    final static int FETCH_QUEUE_LIMIT = 8000;

    public static void main(String[] args) throws IOException,
                                                  InterruptedException,
                                                  URISyntaxException {
        if (args.length != 1) {
            System.err.println("Usage: java -jar crawler.jar <properties-file>");
            return;
        }

        Properties props = new Properties();

        try {
            props.load(new FileInputStream(args[0]));
        } catch (IOException e) {
            System.err.println("Could not read properties file: '" + args[0] + "'");
            return;
        }

        final String domain = props.getProperty("domain");
        final int bundleSize = Integer.parseInt(props.getProperty("bundleSize"));
        final String outputFile = props.getProperty("outputFile");
        logger.info("domain: '" + domain + "'");
        logger.info("bundleSize: '" + Integer.toString(bundleSize) + "'");
        logger.info("outputFile: '" + outputFile + "'");

        JedisPool pool = new JedisPool(new JedisPoolConfig(), "localhost");
        Jedis jedis = pool.getResource();
        HttpFetcher fetcher = new HttpFetcher(new DefaultHttpClient());
        RedisUriQueue fetchQueue = new RedisUriQueue(jedis, "fetchQueue");
        RedisUriQueue usedQueue = new RedisUriQueue(jedis, "uriSet");

        long bundleCount = 0;
        if (jedis.get("bundleCount") != null) {
            bundleCount = jedis.incr("bundleCount");
        } else {
            jedis.set("bundleCount", Long.toString(bundleCount));
        }

        fetchQueue.push(new URI("http://" + domain));

        BundleProtos.Bundle.Builder bundle = BundleProtos.Bundle.newBuilder();

        URI currentUri;
        while ((currentUri = fetchQueue.pop()) != null) {
            if (usedQueue.has(currentUri)) {
                continue;
            }

            try {
                usedQueue.push(currentUri);

                BundleProtos.Document doc = fetcher.fetch(currentUri);
                if (doc == null) {
                    continue;
                }
                bundle.addDocument(doc)
                    .setSize(bundle.getSize() + 1);

                Document html = Jsoup.parse(doc.getContent().toStringUtf8());
                Elements listOfLinks = html.select("a[href]");

                int count = 0;
                for (Element link : listOfLinks) {
                    URI newUri;
                    try {
                        String uriStr = link.attr("href").indexOf("http") == 0 ?
                            link.attr("href") :
                            ("http://" + currentUri.getHost() + link.attr("href"));
                        if (uriStr.indexOf("#") > 0) {
                            uriStr = uriStr.substring(0, uriStr.indexOf("#"));
                        }

                        newUri = new URI(uriStr);

                        if (newUri.getHost() == null) {
                            continue;
                        }

                        if (newUri.getHost().equalsIgnoreCase(domain) == false) {
                            continue;
                        }

                        if (newUri.getPath().indexOf(".pdf") > 0) {
                            continue;
                        }

                        count++;
                        fetchQueue.push(newUri);
                    } catch (URISyntaxException e) {
                        logger.debug("URISyntaxException with href='" + link.attr("href") + "'",
                                     e);
                    }
                }

                logger.info("OK\t" + currentUri.toString());

                if (bundle.getDocumentList().size() >= bundleSize) {
                    bundleCount = jedis.incr("bundleCount");
                    FileOutputStream output = new FileOutputStream(outputFile + "." +
                                                                   Long.toString(bundleCount));
                    bundle.build().writeTo(output);
                    output.close();
                    bundle = BundleProtos.Bundle.newBuilder();

                    logger.info("Bundle #" + Long.toString(bundleCount));
                    logger.info("fetchQueue.size = " + Long.toString(fetchQueue.size()));
                    logger.info("usedQueue.size = " + Long.toString(usedQueue.size()));
                }

                Thread.sleep(400);
            } catch (Exception e) {
                logger.error("Could not fetch URL '" + currentUri + "'", e);
                usedQueue.push(currentUri);
            }
        }

        logger.info("Wrapping up");
        logger.info("Bundle #" + Long.toString(bundleCount));
        logger.info("fetchQueue.size = " + Long.toString(fetchQueue.size()));
        logger.info("usedQueue.size = " + Long.toString(usedQueue.size()));
        FileOutputStream output = new FileOutputStream(outputFile + "." +
                                                       Long.toString(bundleCount));
        bundle.build().writeTo(output);
        output.close();
        bundle = BundleProtos.Bundle.newBuilder();
    }

}
