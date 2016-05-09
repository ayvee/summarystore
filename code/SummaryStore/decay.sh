#!/bin/bash
N=1,000,000
W=100,000
N=$(echo $N|sed 's/,//g')
W=$(echo $W|sed 's/,//g')
for decay in exponential polynomial0 polynomial1 polynomial2 polynomial4 polynomial8
do
	./run.sh PrintDecayFunction $decay $N $W > decay-$decay.tsv
done
gnuplot decay.gp
