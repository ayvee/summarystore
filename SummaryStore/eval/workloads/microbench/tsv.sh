#!/bin/bash
cd $(dirname $0)
ss=../../ssexp
if [[ $# -lt 1 ]]
then
    echo "SYNTAX: $0 <prefix> [metric (default p95)]"
    exit 2
fi
prefix=$1
if [[ $# -ge 2 ]]
then
    metric=$2
else
    metric="p95"
fi

run() {
    # SYNTAX: $normrun $prefix $suffix $metric
    prefix=$1
    suffix=$2
    metric=$3
    toml="$prefix-$suffix.toml"
    tsv="$prefix-$suffix.tsv"
    $ss RunComparison $toml -metrics error:$metric latency:$metric ci-width:$metric ci-miss-rate > $tsv
}

#$ss RunComparison $prefix-count-sum.toml > $prefix-count-sum.tsv
run $prefix count-sum $metric
$ss RunComparison $prefix-bf.toml -metrics error:mean latency:$metric ci-width:$metric ci-miss-rate > $prefix-bf-$metric.tsv
for toml in $prefix-cms20KB*toml # FIXME
do
    #cmsout=$(echo $toml|sed 's/toml$/tsv/')
    #out=$(echo $cmsout|sed "s/$prefix-cms/$prefix-/")
    #$ss RunComparison $toml > $cmsout
    #cat $prefix-count-sum.tsv $prefix-bf.tsv $cmsout > $out
    run $prefix cms20KB $metric 
    cat $prefix-count-sum-$metric.tsv $prefix-bf-$metric.tsv $prefix-cms20KB-$metric.tsv > $prefix-20KB.tsv
done
cd - >& /dev/null
