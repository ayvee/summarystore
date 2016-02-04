load "template.gp"
set datafile separator "\t:_"

set output "varyQueries_error-s.pdf"
set key top left
set title "Median % error vs Zipf shape parameter s"
set xlabel "Zipf shape parameter s"
set ylabel "Median % error"
set logscale y
set mytics 10
plot "varyQueries_summarystore.tsv" u 1:(($5-1)*100) w lp ls 1 t "SummaryStore",\
     "varyQueries_linearstore(4546).tsv" u 1:(($5-1)*100) w lp ls 2 t "LinearStore of same size"
unset logscale y
unset mytics

set term pdfcairo size 6, 6
set ylabel ""
svals="SED_SVALS_SED"

set output "varyQueries_error-size.pdf"
set key top right
set title "Store size vs Median % error"
set xlabel "Median % error"
plot for [i=1:words(svals)] "varyQueries_s" . word(svals, i) . ".tsv" u (($5-1)*100):0:ytic(2) w lp ls i t "s = " . (word(svals, i) == "1.0E-6" ? "0.0" : word(svals, i))

set output "varyQueries_latency-size.pdf"
set key bottom right
set title "Store size vs Median query latency (ms)"
set xlabel "Median query latency (ms)"
plot for [i=1:words(svals)] "varyQueries_s" . word(svals, i) . ".tsv" u 12:0:ytic(2) w lp ls i t "s = " . (word(svals, i) == "1.0E-6" ? "0.0" : word(svals, i))

set output "varyQueries_inflation-count.pdf"
set key top left
set title "N = 10^6\nRandom count query with Zipf-distributed endpoints:\n% error (SummaryStore always overestimates)"
set xlabel "Zipf shape parameter s\n(higher s = stronger recency bias)"
set ylabel "% error"
plot "varyQueries_summarystore.tsv" u 1:(($4-1)*100):(($3-1)*100):(($5-1)*100) w yerrorlines ls 1 notitle

#set output "uniform-cdf.pdf"
#set title "N = 10^6\nCount query with uniform-random endpoints:\nerror distribution"
#set logscale x
#set xlabel "% error"
#set format x "10^{%T}"
#set ylabel "Fraction of queries\nwith % error > x"
#plot "uniform.cdf" u (($1-1)*100):(1-$2) w l ls 1 notitle
