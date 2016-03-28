load "template.gp"
set datafile separator "\t:_"
set term pdfcairo size 7, 3

set xlabel "Storage reduction over linear\\_buckets(1s) baseline"
set key outside
#set logscale x
set xrange [1:100]
set format x "%gx"
#set logscale y

T = "1000"
ls = "1000"
zipfs = "0 1 2"
pvals = "50 95 99 99.9"

do for [li=1:words(ls)] {
l = word(ls, li)
set output "bound-sa_T".T."_l".l."-mean.pdf"
set title "Mean % error\nT = ".T."s, Arrivals = Poisson(".l."/s)"
set ylabel "Query % error"
plot for [i=1:words(zipfs)] "bound-sa_T".T."_l".l."_z" . word(zipfs, i) . "-mean.tsv" u 1:($2*100) w l ls i t (word(zipfs, i) == "0.0" ? "Uniform random queries": "Zipf^2(s = " . word(zipfs, i) . ") queries")
#set title "T = ".T."s, Arrivals = Poisson(".l."/s)"
#set ylabel "Expected accuracy"
#set yrange [0:1]
#set logscale x
#set autoscale x
#plot for [i=1:words(zipfs)] "bound-sa_T".T."_l".l."_z" . word(zipfs, i) . "-mean.tsv" u 1:(1-$2) w l ls i t (word(zipfs, i) == "0.0" ? "Uniform random queries": "Zipf^2(s = " . word(zipfs, i) . ") queries")
#set autoscale y
#set xrange [1:100]
#unset logscale x
}

do for [pi=1:words(pvals)] {
do for [li=1:words(ls)] {
l = word(ls, li)
set output "bound-sa_T".T."_l".l."-".word(pvals, pi)."th.pdf"
set title word(pvals, pi)."th percentile % error\nT = ".T."s, Arrivals = Poisson(".l."/s)"
set ylabel "Query % error"
plot for [i=1:words(zipfs)] "bound-sa_T".T."_l".l."_z" . word(zipfs, i) . "-percentiles.tsv" u 1:(column(1+pi)*100) w l ls i t (word(zipfs, i) == "0.0" ? "Uniform random queries": "Zipf^2(s = " . word(zipfs, i) . ") queries")
}
}
