load "template.gp"
set datafile separator "\t:_"
set term pdfcairo size 7, 3

set xlabel "Storage reduction over linear\\_buckets(1s) baseline"
set ylabel "Expected accuracy (%)"
set key outside
set logscale x
set format x "%gx"
set mxtics 10

T = "600"
ls = "1.0 1000.0"
zipfs = "0.0 0.5 1.0 1.5 2.0"

do for [li=1:words(ls)] {
l = word(ls, li)
set output "bound-sa_T".T."_l".l.".pdf"
set title "T = ".T."s, Arrivals = Poisson(".l."/s)"
plot for [i=1:words(zipfs)] "bound-sa_T".T."_l".l."_z" . word(zipfs, i) . ".tsv" u 1:((1-$2)*100) w l ls i t "Zipf^2(s = " . word(zipfs, i) . ") queries"
}
