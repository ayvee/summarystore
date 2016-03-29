load "template.gp"

set output "opt-relcost.pdf"
set xlabel "age/length distribution Zipf shape parameter s"
set ylabel "cost ratio"
set title "Ratio between cost of fixed scheme and\ncost of optimal windowing scheme"
set key top left
#set logscale x
set xrange [0:3]
set logscale y
plot "opt.tsv" u 1:($4/$2) w lp ls 1 t "pow(2) EH",\
     "opt.tsv" u 1:($3/$2) w lp ls 2 t "LinearStore"
unset logscale x
unset logscale y
set autoscale x
set autoscale y

set output "opt-abscost.pdf"
set xlabel "age/length distribution Zipf shape parameter s"
set ylabel "Mean error\n(estimate minus true value)"
set title "Mean error vs Zipf shape parameter s"
set key top right
plot "opt.tsv" u 1:4 w lp ls 1 t "pow(2) EH",\
     "opt.tsv" u 1:3 w lp ls 2 t "LinearStore",\
     "opt.tsv" u 1:2 w lp ls 3 t "Optimal windowing"
unset logscale x
unset logscale y
set autoscale x
set autoscale y

set output "opt-ratio.pdf"
set ylabel "(oldest window size) /\n(newest window size)"
set title "Ratio of sizes of oldest and newest windows\nwhen using 10 counts to cover 1023 elements"
set key bottom right
plot "opt.tsv" u 1:5 w lp ls 1 t "Optimal windowing scheme",\
     512 w l ls 2 t "pow(2) EH"

#set output "opt-zipf-shape.pdf"
#set ylabel "probability"
#set title "P(some element with age >= 10 is referenced)"
#plot "opt.tsv" u 1:2 w lp ls 1 notitle
