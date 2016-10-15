#!/usr/bin/env python
"""Plot the output of a RunComparison execution to "plot.pdf".
Generates a table with rows/columns = age/length class,
cells = storage vs (some accuracy metric)
"""

import argparse
from collections import OrderedDict, defaultdict
import os
from sh import gnuplot, pdflatex

def is_float(x):
    try:
        float(x)
        return True
    except:
        return False

def process(tsv, metric = None, query = None, enumsize = None):
    with open(tsv) as inf:
        lines = inf.readlines()
        colnames = lines[0][1:].strip().split('\t') # first line is a header that starts with '#'
        column = colnames.index # helper function to find column number by name
        data = [line.strip().split('\t') for line in lines[1:]]
        for i in xrange(len(data)):
            data[i][1] = long(data[i][1]) # store size is a long
            for j in xrange(5, len(colnames)):
                data[i][j] = float(data[i][j]) # all metrics have float values
        unique_vals = lambda colnum: list(OrderedDict.fromkeys(row[colnum] for row in data))
        ages = unique_vals(column('age class'))
        lengths = unique_vals(column('length class'))
        try:
            if metric is None:
                assert len(colnames) == 6
                metric = colnames[5]
            else:
                assert metric in colnames[5:]
        except AssertionError:
            raise AssertionError('-metric must be specified as one of %s' % ' '.join(colnames[5:]))
        try:
            all_queries = unique_vals(column('query'))
            if query is None:
                assert len(all_queries) == 1
                query = all_queries[0]
            else:
                assert query in all_queries
        except AssertionError:
            raise AssertionError('-query must be specified as one of %s' % ' '.join(all_queries))

    pdf_files = 'tmp-A%s-L%s.pdf'
    dat_file = 'tmp-data.tsv'
    gp_file = 'tmp.gp'
    tex_file = 'plot.tex'
    tex_aux_files = ['plot.aux', 'plot.log']
    all_files = [pdf_files % (age, length) for age in ages for length in lengths] + [dat_file, gp_file, tex_file] + tex_aux_files
    pdfs = defaultdict(dict)
    for age in ages:
        for length in lengths:
            have_data = False
            with open(dat_file, 'w') as outf:
                for row in data:
                    if row[column('query')] == query and row[column('age class')] == age and row[column('length class')] == length:
                        have_data = True
                        outf.write("%s\t%s\n" % (row[1], row[column(metric)]))
            if have_data:
                out_file = pdf_files % (age, length)
                pdfs[age][length] = out_file
                with open(gp_file, 'w') as outf:
                    if enumsize is not None:
                        normfact = float(enumsize)
                        plotx = 'set xrange [0:1]'
                    else:
                        normfact = 1
                        plotx = 'set autoscale x'
                    outf.write("""load "template.gp"
set term pdfcairo lw 8
unset grid
unset xtics
unset ytics
set output "{out_file}"
{plotx}
set yrange [0:100]
plot "{dat_file}" u ($1/{normfact}):($2*100) w lp ls 1 notitle
""".format(**locals()))
                gnuplot(gp_file)
    with open(tex_file, 'w') as outf:
        outf.write("""\documentclass[12pt]{article}
\usepackage{graphicx}
\usepackage{palatino}
\usepackage{diagbox}
\usepackage{pdflscape}

\\begin{document}
\\thispagestyle{empty}

\\begin{landscape}
\\begin{table}
\centering
\\begin{tabular}{l|%s}
%%\hline
\\diagbox{age}{length} & %s \\\\
\hline
""" % (\
        ''.join('l' for i in xrange(len(lengths))),\
        ' & '.join(lengths)))
        for age in ages:
            if age not in pdfs:
                continue
            outf.write("%s" % age)
            for length in lengths:
                if length not in pdfs[age]:
                    continue
                outf.write(" & \includegraphics[width=0.15\\textwidth]{%s}" % pdfs[age][length])
            outf.write("""\\\\
%\hline
""")
        if enumsize is not None:
            xaxis = 'sstore/enumeration bytes [0:1]'
        else:
            xaxis = '\# sstore windows'
        if metric[0] == 'p' and is_float(metric[1:]):
            yaxis = '%sth percentile' % metric[1:]
        else:
            yaxis = metric
        yaxis += ' percent error [0:100]'
        outf.write("""
\end{tabular}
\caption{%s query: x-axis %s, y-axis %s}
\end{table}
\end{landscape}

\end{document}""" % (query, xaxis, yaxis))
    pdflatex(tex_file)

    for fil in all_files:
        try:
            os.unlink(fil)
        except OSError: # file doesn't exist
            pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Plot the output of a RunComparison execution')
    parser.add_argument('tsv', help = 'tsv file, output from RunComparison.java')
    parser.add_argument('-metric', help = 'which metric to plot')
    parser.add_argument('-query', help = 'which query to plot')
    parser.add_argument('-enumsize', help = 'size of enumerated store (in # windows)')
    args = parser.parse_args()
    process(**vars(args))
