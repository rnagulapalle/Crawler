package com.seporaitis.crawler.queue;

import java.net.URI;
import java.net.URISyntaxException;

import redis.clients.jedis.Jedis;

public class RedisUriQueue implements UriQueue {
    
    protected final Jedis redis;
    protected final String name;
    
    public RedisUriQueue(Jedis redis, String name) {
        this.redis = redis;
        this.name = name;
    }

    @Override
    public boolean push(URI uri) {
        redis.sadd(this.name, uri.toString());
        return true;
    }

    @Override
    public boolean has(URI uri) {
        if(uri == null) {
            return false;
        }
        return redis.sismember(this.name, uri.toString());
    }

    @Override
    public URI pop() throws URISyntaxException {
        try {
            String value = redis.spop(name);
            if(value == null) {
                return null;
            }
            return new URI(value);
        } catch(URISyntaxException e) {
            return null;
        }
    }

    @Override
    public long size() {
        return redis.scard(this.name);
    }
    
    @Override
    public void clear() {
        redis.del(name);
    }

}
