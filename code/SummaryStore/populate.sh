#!/bin/bash
cp=".:target/SummaryStore-1.0-SNAPSHOT.jar"
for jar in target/lib/*
do
	cp="$cp:$jar"
done

set -e
dstdir="$(dirname $0)/datasets"
N="10,000,000"
N=$(echo $N|sed 's/,//g')
Ds="exponential"
for p in 1 2 3 4 5 6 7 8 9 11 13 15 18 21 24 28 32
do
	Ds="$Ds rationalPower$p,1"
done

for D in $Ds
do
	tmpprefix="$(dirname $0)/N$N.D$D"
	java -Xmx10G -cp $cp com.samsung.sra.DataStoreExperiments.PopulateData -N $N -decay $D -outprefix $tmpprefix
	mv $tmpprefix* $dstdir/
done
