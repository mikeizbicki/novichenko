#!/bin/sh

for host in $(ls /data/common-crawl/warc/); do
    docker-compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.prod.yml -f docker-compose.run.yml run -d --name=$(echo $host | tr ',' '.' ) downloader_cc -c "python3 downloader_warc.py --warc /data/common-crawl/warc/$host/*"
done

# the command:
# $ docker-compose -f docker-compose.run.yml down
# can be used to terminate all the running containers created above
