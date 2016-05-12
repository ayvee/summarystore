#!/usr/bin/env python
"""
Code to generate a windowing with decay function:
	bytes allocated to nth oldest element roughly = 1 / (d * n)^(1/d),
where d \in {1, 2, 3, ...} is a configurable parameter
"""

import argparse
import errno, os, subprocess
import locale
import math

def print_window_seq(N, W, d, outfile):
	if W == 0:
		S = 1
	else:
		k = N / float(W) * (d+2.0) / (d+1.0)
		S = int(math.ceil(N * (d+2.0) / pow(k, d+2)))
	def unstretched_gen():
		b = 1
		while True:
			for i in xrange(pow(b, d)):
				yield b
			b += 1
	unstretched = unstretched_gen()
	def stretched_gen():
		while True:
			w = next(unstretched)
			for i in xrange(0, S):
				yield w
	stretched = stretched_gen()

	with open(outfile, 'w') as outf:
		outf.write("#N = {N}, S = {S}, d = {d}\n#age\tdecay\n".format(**locals()))
		n = 1
		num_windows = 0
		while n < N:
			w = next(stretched)
			assert w > 0
			outf.write("{l}\t{decay}\n{r}\t{decay}\n".format(
				l = n, r = n+w, decay = 1.0 / w))
			n += w
			num_windows += 1
		outf.write("#num_windows = %d\n" % num_windows)

def compute_and_plot(N, W, ds, plotfile = "decay.pdf", logx = False, logy = False, keep_dat = False):
	tmp_files = ["tmp.gp"]
	if not keep_dat: tmp_files += ["decay-d:%d.tsv" % d for d in ds]
	def clean_tmp():
		for tmp in tmp_files:
			try:
				os.remove(tmp)
			except OSError as e:
				if e.errno != errno.ENOENT: #ENOENT is "no such file or directory"
					raise e
	clean_tmp()
	
	try:
		for d in ds:
			print_window_seq(N, W, d, "decay-d:%d.tsv" % d)
	
		if logx:
			xaxis_cmds = "set logscale x\nset xrange [1:{N}]".format(**locals())
		else:
			xaxis_cmds = "set xrange [0:{N}]".format(**locals())
		if logy:
			yaxis_cmds = "set logscale y"
		else:
			yaxis_cmds = ""
		locale.setlocale(locale.LC_ALL, 'en_US')
		Nstr = locale.format("%d", N, grouping = True)
		Wstr = locale.format("%d", W, grouping = True)
		gnuplot_inp = """load "template.gp"
set output "{plotfile}"
{xaxis_cmds}
{yaxis_cmds}
set title {W} == 0 ? "Unscaled decay functions" : "Scaled to N = {Nstr}; W = {Wstr}\\n(only meaningful if N and W are both large)"
set xlabel "Element age"
set ylabel "# bytes allocated to element"
labels="{dvals}"
plot for[i=1:words(labels)] "decay-".word(labels, i).".tsv" u 1:2 w l ls i t word(labels, i)
""".format(dvals = " ".join("d:%d" % d for d in ds), **locals())
		with open("tmp.gp", 'w') as gpfile:
			gpfile.write(gnuplot_inp)
		subprocess.call(["gnuplot", "tmp.gp"])
	finally:
		clean_tmp()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description = __doc__)
	comma_separated_int = lambda s: int(s.replace(',', ''))
	parser.add_argument("N", type = comma_separated_int, help = "size of stream")
	parser.add_argument("ds", metavar = 'd', type = comma_separated_int, nargs = '+', help = "decay rate parameter: decay(n) ~= 1 / (d*n)^(1/d)")
	parser.add_argument("-W", type = comma_separated_int, default = 0, help = "number of windows to scale to (no scaling by default)")
	parser.add_argument("--logx", action = "store_true", help = "use logscale x axis")
	parser.add_argument("--logy", action = "store_true", help = "use logscale y axis")
	parser.add_argument("--plotfile", default = "decay.pdf", help = "name of plot output file (defaults to decay.pdf)")
	parser.add_argument("--keep-dat", dest = "keep_dat", action="store_true", help = "do not delete window length data files after plotting")
	args = parser.parse_args()
	compute_and_plot(**vars(args))
