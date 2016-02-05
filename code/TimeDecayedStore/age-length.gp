#load "template.gp"
set terminal pdfcairo font "Calibri,9" linewidth 2 rounded fontscale 1.0 size 10, 6

set xlabel "age"
set ylabel "length"
set title "When does SummaryStore beat LinearStore of same size?"
set logscale x
set logscale y
set mxtics 10
set mytics 10
set output "age-length-scatter.pdf"
plot "age-length-scatter.tsv" u 1:2 w p ls 1 notitle

set pm3d map #interpolate 0, 0
set palette defined (0 "black", 1 "green")
set title "Probability that SummaryStore beats LinearStore of same size\non a random count query"
set xlabel "age"
set ylabel "length" offset 3, 0

inf = "age-length-matrix.tsv"
logN = 6

set logscale x
set logscale y
set xrange [1:10**logN]
set yrange [1:10**logN]
set tics scale 0
set xtics ()
set ytics ()
set for [i=0:logN] xtics add (sprintf("%d-%d", 10**i, 10**(i+1) - 1) sqrt(10) * (10**i))
set for [i=0:logN] ytics add (sprintf("%d-%d", 10**i, 10**(i+1) - 1) sqrt(10) * (10**i))
set output "age-length.pdf"
#plot inf matrix rowheaders columnheaders w image pixels notitle
splot inf nonuniform matrix u 2:1:3 w pm3d notitle
