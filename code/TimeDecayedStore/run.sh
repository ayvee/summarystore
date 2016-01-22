#!/bin/bash
cp=target/TimeDecayedStore-1.0-SNAPSHOT.jar
for jar in target/lib/*
do
	cp="$cp:$jar"
done
java -cp $cp com.samsung.sra.DataStore.TimeDecayedStore
