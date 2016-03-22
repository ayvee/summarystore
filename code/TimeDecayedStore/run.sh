#!/bin/bash
cp=target/TimeDecayedStore-1.0-SNAPSHOT.jar
for jar in target/lib/*
do
	cp="$cp:$jar"
done
#java -cp $cp com.samsung.sra.DataStore.TimeDecayedStore

#java -cp $cp com.samsung.sra.DataStoreExperiments.VaryN | tee varyN.tsv
#java -cp $cp com.samsung.sra.DataStoreExperiments.VaryQueries | tee varyQueries.tsv
#java -cp $cp com.samsung.sra.DataStoreExperiments.AgeLengthEffect > age-length-matrix.tsv 2> age-length-scatter.tsv
#java -cp $cp com.samsung.sra.WindowingOptimizer.UpperBoundOptimizer 2> opt.log | tee opt.tsv
#java -cp $cp com.samsung.sra.WindowingOptimizer.StorageVsAccuracy
java -cp $cp com.samsung.sra.WindowingOptimizer.AgeLengthVsAccuracy

#./plot.sh
#gnuplot age-length.gp
#gnuplot opt.gp
