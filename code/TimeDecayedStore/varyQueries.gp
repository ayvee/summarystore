load "template.gp"
set datafile separator "\t:_"

set logscale x
set output "varyQueries_inflation-count.pdf"
set key top left
set title "Zipf-random count query: inflation\n(TDS count estimate divided by ES true count)"
set xlabel "Zipf shape parameter s\n(higher s = stronger recency bias)"
set ylabel "Inflation"
plot "varyQueries.tsv" u 1:4:3:5 w yerrorlines ls 1 notitle
