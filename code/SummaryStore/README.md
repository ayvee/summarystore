INSTALL
==============

1. Install RocksDB

    git clone https://github.com/facebook/rocksdb.git
    cd rocksdb
    Set JAVA_HOME
    make rocksdbjavastatic
    mvn install:install-file -Dfile=java/target/rocksdbjni-4.8.0-linux64.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=4.4 -Dpackaging=jar -DgeneratePom=true

EXPERIMENTS
==============

Each experiment runs in three phases:

1. PopulateData: populate a summary store. Usually run multiple times, to generate several stores holding the same data with different decay functions.
2. GenerateWorkload: generate a list of queries and their true answers
3. CompareDecayFunctions: run the generated workload against each of the stores from step 1 to build a profile, a CDF over the error distribution for each of a specified number of age/length classes.

Run any class with -h (e.g. ./run.sh PopulateData -h) to see supported operations.

Quickstart (more detailed instructions later):

    mkdir datasets
    export T=1,000,000 (say)
    ./populate.sh $T datasets simplecountUPPER_BOUND # runs PopulateData and GenerateWorkload
    ./run.sh CompareDecayFunctions datasets $T

(FIXME: right now the generated workload always only tests the very first operator, operator #0, which it assumes is a count)
