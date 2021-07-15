## Roadmap

Ops: 
1. Automatically run initialization commands on coordinator
1. Remove unnecessary sql from the workers (they currently run the same sql as coordinator)
1. Combine the metahtml and metahtml_view tables
X. Generate default data that comes from many domains

Rollups:
1. Add using syntax

Search:
X. rollup tables based on citus distributed tables don't work
1. first search hostnames, then search urls from each hostname
1. add fancy personalized pagerank

Features:
1. links rollup with both src and dest
1. links rollup for content

## Known issues

1. many organizations labeled by allsides dataset have multiple domains; for example:
    1. mrc.org is in allsides directly, but it's really multiple domains
    1. politico.com and politico.eu

1. aggregators:
    1. bignewsnetwork.com
