"""A query runner running queries of the form
    [tstart, tend) optional_filter aggr
plus other misc useful functions.
"""

def run_query_influx(client, metric, tstart, tend, aggr, filters = None):
    aggr = aggr.lower()
    proj = {'sum': "SUM(value)", 'count': "COUNT(*)", 'enum': "*"}[aggr]
    src = '"%s"' % metric
    filt = "time >= %ds" % tstart
    if tend is not None: filt += " AND time < %ds" % tend
    if filters:
        for k, v in filters.iteritems():
            filt += " AND %s = '%s'" % (k, v)
    query = 'SELECT {proj} FROM {src} WHERE {filt}'.format(**locals())
    results = client.query(query)[metric]
    if aggr != 'enum':
        for row in results:
            ans = row[{'sum': 'sum', 'count': 'count_value'}[aggr]]
            #print "[%s, %s) %s %s = %s" % (tstart, tend, filters, aggr, ans)
            return ans
        else:
            # empty resultset, so sum or count = 0
            return 0
    else:
        return results

def run_query_opentsdb(client, metric, tstart, tend, aggr, filters = None):
    # TODO. OpenTSDB only enumerates, so have to manually implement filter/aggr
    raise Exception

def query(client, metric, queries, mode = 'influx'):
    """arg should be either one query tuple
        (tstart, tend, aggr[, optional_filters])
    or an iterable of query tuples. Each tuple corresponds to the query
        [tstart, tend) optional_filters aggr
    tstart and tend: unix timestamps, tend can be None
    aggr: one of 'sum', 'count' or 'enum'
    optional_filters: a map of key = value filters"""
    runner = globals()["run_query_" + mode]
    if isinstance(queries, tuple):
        # one query, return answer
        return runner(client, metric, *queries)
    else:
        # bag of queries, return list of answers
        return [runner(client, metric, *q) for q in queries]

def get_query_rms(client, base_metric, sampled_metric, queries):
    """Given either one sum/count query or a bag of sum/count queries, return
    the percentage RMS error ||estimate - actual|| / ||actual||, where
    ||v|| = sqrt(\sum_i v_i^2)"""
    # TODO: verify there were no enumeration queries
    import math
    actual = query(client, base_metric, queries)
    est = query(client, sampled_metric, queries)
    if isinstance(actual, (int, long, float)): # single query, not bag
        actual = [actual]
        est = [est]
    err = 0.0
    base = 0.0
    for actual_i, est_i in zip(actual, est):
        #print (actual_i, est_i)
        if actual_i > 0:
            err += (est_i - actual_i) ** 2
            base += actual_i ** 2
    return math.sqrt(err / base)


def unix_ts(*args):
    """Get unix timestamp for date. Accepts same args as Python's
    datetime.datetime, viz
    year, month, day[, hour[, minute[, second[, microsecond[, tzinfo]]]]]"""
    import datetime
    return int(datetime.datetime(*args).strftime("%s"))


def allvars():
    """globals() + locals(): returns a dict containing the current scope's
    local and global variables"""
    return dict(globals(), **locals())


class Statistics(object):
    def __init__(self, nbins = 1000):
        self.nbins = nbins
        self.count = 0
        self.sum = 0
        self.min = float('inf')
        self.max = float('-inf')
        self.vals = []
        self.cdf = None

    def update(self, val):
        self.count += 1
        self.sum += val
        self.min = min(self.min, val)
        self.max = max(self.max, val)
        self.vals.append(val)
        self.cdf = None

    def mean(self):
        return self.sum / self.count if self.count > 0 else 0

    def _bin_left(self, binnum):
        "left endpoint of the binnum-th CDF bin"
        if binnum < 0:
            return self.min
        elif binnum >= self.nbins:
            return self.max
        else:
            return self.min + (self.max - self.min) * binnum / float(self.nbins)

    def _binnum(self, val):
        b = int((val - self.min) / float(self.max - self.min) * self.nbins)
        if b < 0:
            return 0
        elif b >= self.nbins:
            return self.nbins - 1
        else:
            return b

    def _build_cdf(self):
        self.cdf = [[self._bin_left(b), self._bin_left(b+1), 0] for b in xrange(self.nbins)]
        for v in self.vals:
            self.cdf[self._binnum(v)][2] += 1
        for b in xrange(self.nbins):
            if b > 0:
                self.cdf[b][2] += self.cdf[b-1][2]
            self.cdf[b][2] /= float(self.count)

    def get_cdf_str(self):
        if self.cdf is None:
            self._build_cdf()
        s = "#Mean = %s\n" % self.mean()
        s += "#bin left\tbin right\tcumulative density\n"
        s += "\n".join("\t".join(str(v) for v in e) % e for e in self.cdf)
        s += "\n"
        return s

    def print_cdf(self):
        print self.get_cdf_str(),

    def write_cdf(self, filename):
        with open(filename, 'w') as outf:
            outf.write(self.get_cdf_str())


if __name__ == '__main__':
    print unix_ts(2000, 1, 1)
    #from influxdb import InfluxDBClient
    #client = InfluxDBClient(database = 'tsm')
    #print query(client, 'backup', (unix_ts(2000, 1, 1), unix_ts(2001, 1, 1), 'count'))
    stats = Statistics()
    for i in xrange(1000):
        stats.update(i)
    for i in xrange(10):
        stats.update(990)
    stats.print_cdf()
