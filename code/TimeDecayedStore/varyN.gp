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
plot "varyN_dbsize.tsv" u 1:($2/1000) w lp ls 1 t "SummaryStore",\
     "" u 1:($3/1000) w lp ls 2 t "Enumeration"

set output "varyN_latency-append.pdf"
set key center right
set ylabel "Time (ms)"
set title "Time to append 1 element (ms)"
#plot "varyN_latency-append.tsv" u 1:2:3 w yerrorlines ls 1 t "SummaryStore",\
#     "" u 1:4:5 w yerrorlines ls 2 t "Enumeration"
plot "varyN_latency-append.tsv" u 1:2 w lp ls 1 t "SummaryStore",\
     "" u 1:4 w lp ls 2 t "Enumeration"

set output "varyN_latency-query.pdf"
set key top left
set ylabel "Time (ms)"
set title "Random count query: time to process 1 query (ms)"
#plot "varyN_latency-query.tsv" u 1:4:3:5 w yerrorlines ls 1 t "SummaryStore",\
#     "" u 1:11:10:12 w yerrorlines ls 2 t "Enumeration"
plot "varyN_latency-query.tsv" u 1:6 w lp ls 1 t "SummaryStore",\
     "" u 1:13 w lp ls 2 t "Enumeration"

set output "varyN_latency-query_zoomed.pdf"
set yrange [0:1]
replot
set autoscale y

set output "varyN_inflation-count.pdf"
set key top left
set ylabel "% error"
set title "Count query with uniform-random endpoints:\n% error (SummaryStore always overestimates)"
plot "varyN_inflation-count.tsv" u 1:(($4-1)*100):(($3-1)*100):(($5-1)*100) w yerrorlines ls 1 notitle
