/**
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
package net.seporaitis.crawler.queue;

import java.net.URI;
import java.net.URISyntaxException;


/**
 * URI Queue
 */
public interface UriQueue {

    /**
     * Add URI to the queue.
     *
     * Return true on success.
     *
     * @param URI uri
     *
     * @return boolean
     */
    public boolean push(URI uri);

    /**
     * Check if URI exists in the queue.
     *
     * @param URI uri
     *
     * @return boolean
     */
    public boolean has(URI uri);

    /**
     * Pop an URI from queue.
     *
     * @return URI
     *
     * @throws URISyntaxException
     */
    public URI pop();

    /**
     * Return the size of the queue.
     *
     * @return long
     */
    public long size();

    /**
     * Clear the queue.
     */
    public void clear();

}
