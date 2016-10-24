#!/usr/bin/env python
import datetime, time
import random

from config import *

def generate(outfile, bytes_distr, backup_failure_prob = lambda nodeID: 0, backups_per_day = 1):
    with open(outfile, 'w') as outf:
        for day in xrange(num_days):
            print "day %d" % day
            base_ts = start_ts + day * 86400
            for i in xrange(backups_per_day):
                for nodeID in xrange(0, num_nodes):
                    if random.uniform(0, 1) > backup_failure_prob(nodeID): # backup didn't fail
                        ts = (base_ts + i * 86400 / backups_per_day) * (10 ** 9) + nodeID
                        outf.write("%s,%s,%s\n" % (ts + nodeID, nodeID, long(bytes_distr())))

def filesize():
    "return filesize in KB"
    mb = 1024
    U = random.uniform(0, 1)
    if U < 0.22: # 0-1 MB
        return random.randint(0, 1024 - 1)
    elif U < 0.48: # 1-10 MB
        return random.randint(1024, 10240 - 1)
    elif U < 0.66: # 10-100 MB
        return random.randint(10240, 102400 - 1)
    elif U < 0.84: # 0.1-1 GB
        return random.randint(102400, 1024 * 1024 - 1)
    else: # 1-10 GB
        return random.randint(1024 * 1024, 10 * 1024 * 1024 - 1)

if __name__ == '__main__':
    random.seed(0)
    generate("tsm.sstore",
            bytes_distr = filesize,
            backups_per_day = 24,
            backup_failure_prob = lambda nodeID: 0.01)
