#!/bin/bash
cp=".:target/SummaryStore-1.0-SNAPSHOT.jar"
for jar in target/lib/*
do
	cp="$cp:$jar"
done

set -e
dstdir="/mnt/hdd/sstore"
N="100,000,000"
Ws="10,000 20,000 50,000 100,000 200,000 500,000 1,000,000 2,000,000 5,000,000 10,000,000 20,000,000 50,000,000"
N=$(echo $N|sed 's/,//g')
Ws=$(echo $Ws|sed 's/,//g')
Ds="exponential polynomial0 polynomial1 polynomial2 polynomial4 polynomial8"

for W in $Ws
do
	for D in $Ds
	do
		tmpprefix="$(dirname $0)/N$N.W$W.D$D"
		java -Xmx28G -cp $cp com.samsung.sra.DataStoreExperiments.PopulateData -N $N -W $W -decay $D -outprefix $tmpprefix
		mv $tmpprefix* $dstdir/
	done
done
