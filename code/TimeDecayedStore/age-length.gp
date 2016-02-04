#load "template.gp"
set terminal pdfcairo font "Calibri,9" linewidth 2 rounded fontscale 1.0

set pm3d map #interpolate 0, 0
set palette defined (0 "black", 1 "green")
set title "Probability that SummaryStore beats LinearStore of same size\non a random count query"
set xlabel "age"
set ylabel "length\n"
set format x "10^%T"
set format y "10^%T"
set logscale x
set logscale y
set mxtics 10
set mytics 10
set xrange [1:1000000]
set yrange [1:1000000]
set output "age-length.pdf"
splot "age-length-matrix.tsv" nonuniform matrix u 2:1:3 w pm3d notitle
