load "template.gp"
set datafile separator "\t:_"

set xlabel "Size of store (# elements)"
set logscale x
#set xrange [1:]
set decimal locale
set format x "10^%T"
set format y "%'g"

set output "varyN_dbsize.pdf"
set key top left
set ylabel "Total size (MB)"
set title "Store size (on disk)"
plot "varyN_dbsize.tsv" u 1:($2/1000) w lp ls 1 t "TDS",\
     "" u 1:($3/1000) w lp ls 2 t "ES"

set output "varyN_latency-append.pdf"
set key top right
set ylabel "Time (ms)"
set title "Time to append 1 element (ms)"
#plot "varyN_latency-append.tsv" u 1:2:3 w yerrorlines ls 1 t "TDS",\
#     "" u 1:4:5 w yerrorlines ls 2 t "ES"
plot "varyN_latency-append.tsv" u 1:2 w lp ls 1 t "TDS",\
     "" u 1:4 w lp ls 2 t "ES"

set output "varyN_latency-query.pdf"
set key top left
set ylabel "Time (ms)"
set title "Random count query: time to process 1 query (ms)"
#plot "varyN_latency-query.tsv" u 1:4:3:5 w yerrorlines ls 1 t "TDS",\
#     "" u 1:11:10:12 w yerrorlines ls 2 t "ES"
plot "varyN_latency-query.tsv" u 1:6 w lp ls 1 t "TDS",\
     "" u 1:13 w lp ls 2 t "ES"

set output "varyN_inflation-count.pdf"
set key top left
set ylabel "Inflation"
set title "Random count query: inflation\n(TDS count estimate divided by ES true count)"
plot "varyN_inflation-count.tsv" u 1:4:3:5 w yerrorlines ls 1 notitle
