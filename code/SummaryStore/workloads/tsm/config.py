db = "tsm"
metric = "backup"
sampled_metric = "sampled-backup"
start_ts = 946684800 # unix_ts(2000-01-01)
num_days = 365
num_nodes = 10000
end_ts = start_ts + (num_days - 1) * 86400
