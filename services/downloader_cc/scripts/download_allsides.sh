#!/bin/bash

surts=$(PGPASSWORD=$DB_PASSWORD psql --host=db --user=novichenko -c 'SELECT DISTINCT url_host_surt(url) as host_surt from mediabiasfactcheck order by host_surt asc offset 1500;' | grep -E ',')

for surt in $surts; do
    surt=$(echo $surt | tr ')' ' ')
    echo $surt
    scripts/download_cdxs.sh $surt
done
