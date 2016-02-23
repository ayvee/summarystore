#!/bin/bash
cut -f1,2,3 varyN.tsv > varyN_dbsize.tsv
cut -f1,4,5 varyN.tsv > varyN_latency-append.tsv
cut -f1,6 varyN.tsv > varyN_inflation-count.tsv
cut -f1,8,9 varyN.tsv > varyN_latency-query.tsv
gnuplot varyN.gp

#svals=$(cut -f1 varyQueries.tsv|grep -v '#'|uniq|paste -sd' ' -)
svals="1.0E-6 1.0"
sed "s/SED_SVALS_SED/$svals/g" varyQueries.gp > filled-varyQueries.gp
for s in $svals
do
	grep "^$s\t" varyQueries.tsv > varyQueries_s$s.tsv
done
nvals="summarystore linearstore(4546)"
for n in $nvals
do
	grep $n varyQueries.tsv > varyQueries_$n.tsv
done
gnuplot filled-varyQueries.gp
rm filled-varyQueries.gp

gnuplot age-length.gp
