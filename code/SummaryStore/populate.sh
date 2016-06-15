#!/bin/bash
if [ $# -lt 3 ]
then
	echo "SYNTAX: $0 directory T operator [operator ...]"
	exit 2
fi
Xmx=`./xmx.sh`
cp=".:target/SummaryStore-1.0-SNAPSHOT.jar"
for jar in target/lib/*
do
	cp="$cp:$jar"
done

set -e
outdir=$1
shift
T=$1
shift
Ds="exponential2"
#for p in 1 2 3 4 5 6 7 8 9 11 13 15 18 21 24 28 32
#do
#	Ds="$Ds rationalPower$p,1"
#done

for D in $Ds
do
	java -Xmx$Xmx -cp $cp com.samsung.sra.DataStoreExperiments.PopulateData -cachesize 10,000,000 $outdir $T $D $*
done
java -Xmx$Xmx -cp $cp com.samsung.sra.DataStoreExperiments.GenerateWorkload $outdir $T $*
