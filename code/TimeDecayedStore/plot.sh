#!/bin/bash
cut -f1,2,3 varyN.tsv > varyN_dbsize.tsv
cut -f1,4,5 varyN.tsv > varyN_latency-append.tsv
cut -f1,6 varyN.tsv > varyN_inflation-count.tsv
cut -f1,8,9 varyN.tsv > varyN_latency-query.tsv
gnuplot varyN.gp

gnuplot varyQueries.gp
