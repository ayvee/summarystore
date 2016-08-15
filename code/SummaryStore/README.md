INSTALL
==============

1. Install RocksDB (needs to be done on each OS separately because the jar contains an embedded .so)

        git clone https://github.com/facebook/rocksdb.git
        cd rocksdb
        Set JAVA_HOME
        make rocksdbjavastatic
        mvn install:install-file -Dfile=java/target/rocksdbjni-4.8.0-linux64.jar -DgroupId=org.rocksdb -DartifactId=rocksdbjni -Dversion=4.4 -Dpackaging=jar -DgeneratePom=true

2. mvn package

EXPERIMENTS
==============

Experiments are defined by a combination of (dataset, query workload, decay functions):

* Dataset: implement the StreamGenerator interface to define a new dataset. The code currently has implementations for
 a random dataset generator (using iid interarrival time and value distributions) and a generator that replays the Google
 trace.
* Query workload: implement WorkloadGenerator to define a new workload.
* Decay functions: defined inside the DataStore package. Most of our experiments will use RationalPowerWindowing.

Each experiment runs in three phases:

1. PopulateData: populate a summary store. Usually run multiple times, to generate several stores holding the same data
 with different decay functions.
2. PopulateWorkload: generate a list of queries and their true answers (queries are binned into several classes, e.g. by
 age/length)
3. CompareDecayFunctions: run the generated workload against each of the stores from step 1 to build a profile, a CDF
 over the error distribution for each class
    * Run CompareDecayFunctions with -weight and -metric arguments to output an aggregate accuracy score for each
     decay function, i.e. a storage vs accuracy plot. Error profiles are cached on disk, so trying alternate weight and
     metric combinations (after CompareDecayFunctions has been run once) shouldn't take much time.

Run any class with -h (e.g. ./run.sh PopulateData -h) to see supported parameters.

Quickstart (more detailed instructions later):

    mkdir datasets
    export T=1,000,000 # (say)
    ./populate.sh $T datasets simplecountUPPER_BOUND # runs PopulateData and GenerateWorkload
    ./run.sh CompareDecayFunctions datasets $T

(FIXME: right now the generated workload always only tests the very first operator, operator #0, which it assumes is a
 count)
