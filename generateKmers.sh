#!/bin/sh

TITLE="Kmer features extraction @ $HOSTNAME"
RIGHT_NOW=$(date +"%x %r %Z")
TIME_STAMP="Updated on $RIGHT_NOW by $USER"

module purge

module load python/3.8.1

python ./generateKmers.py -d $1 -n 32 -p 32 -k 14
