#!/usr/bin/env python
import re
import glob
import numpy
import weighted

def get_percentiles(filename, percentiles):
	vals = numpy.loadtxt(filename)
	errors = vals[:,2]
	weights = vals[:,3]
	return {p: weighted.quantile(errors, weights, p * .01) for p in percentiles}

if __name__ == '__main__':
	T = 1000
	l = 1000
	percentiles = [50, 95, 99, 99.9]
	files = glob.glob("bound-al_T%s_l%s_*.tsv" % (T, l))
	zipfs = sorted(list(set(int(re.findall('_z(\d+)_', fil)[0]) for fil in files)))
	ratios = sorted(list(set(int(re.findall('_s(\d+)\.', fil)[0]) for fil in files)))
	for z in zipfs:
		outfile = "bound-sa_T%s_l%s_z%s-percentiles.tsv" % (T, l, z)
		with open(outfile, 'w') as outf:
			outf.write("#storage ratio\t50th\t95th\t99th\t99.9th\n")
			for s in ratios:
				infile = "bound-al_T%s_l%s_z%s_s%s.tsv" % (T, l, z, s)
				pvals = get_percentiles(infile, percentiles)
				outf.write("%s\t%s\n" % (s, "\t".join(str(pvals[p]) for p in percentiles)))
