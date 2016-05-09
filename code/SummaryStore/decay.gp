load "template.gp"

set output "decay.pdf"
set xlabel "Element age"
set ylabel "# bytes allocated to element"
plot "decay-exponential.tsv" u 1:2 w l ls 1 t "Exponential",\
     "decay-polynomial0.tsv" u 1:2 w l ls 2 t "Polynomial(0)",\
     "decay-polynomial1.tsv" u 1:2 w l ls 3 t "Polynomial(1)",\
     "decay-polynomial4.tsv" u 1:2 w l ls 5 t "Polynomial(4)"
