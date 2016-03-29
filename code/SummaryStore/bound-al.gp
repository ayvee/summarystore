load "template.gp"
#set terminal pdfcairo font "Calibri,9" linewidth 2 rounded fontscale 1.0 size 10, 6
set terminal pngcairo font "Calibri,15" linewidth 2 rounded fontscale 1.0# size 10, 6

T = "1000"
l = "1000"
zipfs = "0 1 2"
ratios = "10 100"

do for [zi=1:words(zipfs)] {
do for [ri=1:words(ratios)] {
z = word(zipfs, zi)
r = word(ratios, ri)
inprefix = "bound-al_T".T."_l".l."_z".z."_s".r
outprefix = "bound-al_T".T."_l".l."_z".z."_s".r

rgb(v) = int(v * 255) * 65536 + int((1-v) * 255) * 256
#rgb(v) = int((1-v) * 255) * 256

set title "Error at ".r."x storage savings, windowing tuned to Zipf^2(".z.") workload\nT = ".T."s, Arrivals = Poisson(".l."/s)"

set xlabel "age"
set ylabel "length"# offset 3, 0
set logscale x
set logscale y
set output outprefix.".png"
plot inprefix.".tsv" u 1:2:(rgb($3)) w p pt 5 ps .1 lc rgb variable notitle
unset logscale x
unset logscale y
}
}

do for [ri=1:words(ratios)] {
r = word(ratios, ri)

set title "Unnormalized error distribution at ".r."x storage savings\nT = ".T."s, Arrivals = Poisson(".l."/s)"
set xlabel "Error threshold (%)"
set ylabel "Fraction of queries with error ≥ threshold"
set xrange [0:100]
set yrange [0:1]
set output "bound-al_T".T."_l".l."_s".r."-cdf.png"
plot for [zi=1:words(zipfs)] "bound-al_T".T."_l".l."_z".word(zipfs, zi)."_s".r.".cdf" u ($1*100):(1-$2) w l ls zi t "Zipf^2(".word(zipfs, zi).") windowing"
set autoscale x
set autoscale y
}
