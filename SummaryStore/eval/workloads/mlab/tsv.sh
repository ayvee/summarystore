#!/bin/bash
for toml in server*KB.toml
do
    tsv="mlab-$(echo $toml|sed 's/toml$/tsv/')"
    ../../ssexp RunComparison $toml -force-run > $tsv
done
