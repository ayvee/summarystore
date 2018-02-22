#!/usr/bin/env python
from influxdb import InfluxDBClient
import random

from config import *
from util import *

day = 86400
now = end_ts

# format: (tstart, tend, aggr[, optional nodeID filter])
queries = {
        ### fixed sliding window
        1:  (now - 7 * day, None, 'sum'),
        3:  [(now - 7 * day, None, 'sum', {'node': nodeID})
               for nodeID in xrange(num_nodes)],
        6:  (now - day, None, 'sum'),
        ### some date
        4:  lambda tstart:
            [(tstart, tstart + day, 'sum', {'node': nodeID})
               for nodeID in xrange(num_nodes)],
        5:  lambda tstart:
            (tstart, tstart + day, 'enum'),
        7:  lambda tstart:
            (tstart, tstart + day, 'sum'),
        27: lambda tstart:
            (tstart, tstart + day, 'count'),
        ### some month
        2:  lambda tstart:
            (tstart, tstart + 30 * day, 'sum'),
        #### all time (q15 = "run every possible q4")
        #15: [(start_ts + d * day, start_ts + (d+1) * day, 'sum', {'node': nodeID})
        #       for d in xrange(num_days)
        #       for nodeID in xrange(num_nodes)],
}
for i, q in queries.iteritems():
    globals()["q%d" % i] = q
#print rms(q27(unix_ts(2005, 1, 1)))
#print rms(q15)


date_bin_sizes = [1, 6, 23, 335, 3288]
# convert to list of [bin_start_date, bin_end_date)
date_bin_extents = reduce(lambda bins, size: bins + [(bins[-1][1], bins[-1][1] + size)], date_bin_sizes, [(0, 0)])[1:]

def rand_date():
    extent = random.choice(date_bin_extents)
    return extent[0] + random.randint(0, extent[1] - extent[0] - 1)


client = InfluxDBClient(database = db)
rms = lambda q: get_query_rms(client, metric, sampled_metric, q)

for i in 1, 6: # sliding window queries
    print "q%s\t%s" % (i, rms(queries[i]))
N = 100
for i in 7, 27: # some date
    stats = Statistics()
    for n in xrange(N):
        stats.update(rms(queries[i](start_ts + rand_date() * 86400)))
    #stats.print_cdf()
    print "q%s\t%s" % (i, stats.mean())
