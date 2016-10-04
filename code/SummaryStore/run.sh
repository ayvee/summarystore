#!/bin/bash
if [ $# -lt 1 ]
then
	echo "SYNTAX: $0 className [classArgs]"
	exit 2
fi
root=`dirname $0`
Xmx=`./xmx.sh`
cp="$root:$root/target/SummaryStore-1.0-SNAPSHOT.jar"
for jar in $root/target/lib/*
do
	cp="$cp:$jar"
done
cp="$cp:scripts" # put all scripts on classpath, so they can be found with class.getResource()

className=$1
shift
java -ea -Xmx$Xmx -cp $cp com.samsung.sra.DataStoreExperiments.$className $*
#java -cp $cp com.samsung.sra.WindowingOptimizer.UpperBoundOptimizer 2> opt.log | tee opt.tsv
