package com.seporaitis.crawler.queue;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * URI queue
 * 
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
public interface UriQueue {
    
    public boolean push(URI uri);
    public boolean has(URI uri);
    public URI pop() throws URISyntaxException;
    public long size();
    public void clear();

}
