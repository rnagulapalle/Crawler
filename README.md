# How to run the Crawler

## Get set

Create crawler.properties file and fill out fields:

    domain - the starting page and domain to crawl
    bundleSize - number of response pages per bundle file
    outputFile - path to the output file (bundles will have dot-number
    appended, e.g. /tmp/crawl.0)

## Ready

Build the project && install Redis

    $ mvn package
    $ brew install redis # I use Homebrew package manager, use another if you're on linux

## Go!

    $ redis-server
    $ java -jar target/crawler-1.0-SNAPSHOT-jar-with-dependencies.jar crawler.properties

## Enjoy

After first 'bundleSize' number of responses are gathered (console
output will say), check to see the 'outputFile' exists. There are some
responses serialized in a Bundle. Bundle is just a serializable
Protocol Buffer structure defined in
<code>src/main/protobuf/crawler.proto</code>.


# Wishlist

1. Allow wildcard domains (e.g. '*.example.com'). This will require
entry URL then.

# Warranty & Liability

Although the crawler is not completely stupid and tries not to DDoS
the website it is crawling, I shall nat be held responsible for any
harm you do. :-)

# License

I might get real license description in the future, but until then -
this code is free to play around & use. Just notify me if you have
made something cool out of it. Thanks!
