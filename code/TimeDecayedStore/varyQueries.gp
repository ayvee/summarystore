load "template.gp"
set datafile separator "\t:_"

set logscale x
set xtics (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
set output "varyQueries_inflation-count.pdf"
set key top left
set title "Random count query with Zipf-distributed endpoints:\n% error (TDS always overestimates)"
set xlabel "Zipf shape parameter s\n(higher s = stronger recency bias)"
set ylabel "% error"
plot "varyQueries.tsv" u 1:(($4-1)*100):(($3-1)*100):(($5-1)*100) w yerrorlines ls 1 notitle
