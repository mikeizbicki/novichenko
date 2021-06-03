#!/bin/bash

surt=$1

scripts/download_indexes.sh
scripts/download_cdxs.sh $surt
scripts/download_warcs.sh $surt
