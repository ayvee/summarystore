#!/bin/bash
if [ $# -lt 1 ]
then
	echo "SYNTAX: $0 className [classArgs]"
	exit 2
fi
Xmx=`./xmx.sh`
cp=".:target/SummaryStore-1.0-SNAPSHOT.jar"
for jar in target/lib/*
do
	cp="$cp:$jar"
done

className=$1
shift
java -ea -Xmx$Xmx -cp $cp com.samsung.sra.DataStoreExperiments.$className $*
#java -cp $cp com.samsung.sra.WindowingOptimizer.UpperBoundOptimizer 2> opt.log | tee opt.tsv
