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

To run an experiment, define a dataset and a workload by implementing, respectively, the StreamGenerator and
WorkloadGenerator interfaces, and then run the following scripts:

1. PopulateData: populate a set of SummaryStores holding the same data with different decay functions.
2. PopulateWorkload: generate a workload, a list of queries along with their true answers. Queries are binned into one
 or more classes, e.g. by age/length.
3. RunComparison: run the generated workload against each of the stores from step 1 to build a profile, a CDF over the
 error distribution for each class.
    * Specify -weight and -metric arguments to RunComparison to output an aggregate accuracy score for each
     decay function, i.e. a storage vs accuracy plot. Error profiles are cached on disk, so trying alternate weight and
     metric combinations (after RunComparison has been run once) shouldn't take much time.

All three scripts take a single config file as argument, specifying all parameters. Script format is explained in
[example.toml](example.toml) and defined in Configuration.java.

    ./run.sh PopulateData example.toml
    ./run.sh PopulateWorkload example.toml
    ./run.sh RunComparison example.toml -weight uniform -metric Mean # storage vs avg error
    ./run.sh RunComparison example.toml -weight uniform -metric p95 # storage vs 95th %ile error

When implementing a new StreamGenerator or WorkloadGenerator, you must define a constructor with signature
Generator(Toml params): this is how arguments from the config file will be passed to your class. See e.g.
RandomStreamGenerator and RandomWorkloadGenerator for examples.
