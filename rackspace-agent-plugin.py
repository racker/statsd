#!/usr/bin/env python

import os.path
import sys
import glob
import json

ck_metrics = []

METRIC_TYPE_MAP = {
    'counters': 'float',
}

def output_check_status(status):
    ck_metrics.append("status %s" % (status))

    if status is "err":
        sys.exit(msg)

def output_metrics(metrics):
    """
    Outputs the parsed metrics to the agent.
    """
    # TODO these need to work for a few different types
    for metric_type in ("counters","timers","gauges"):
        metric = metrics.get(metric_type)
	#print metric
        if metric is None:
            continue
        for name, val in ((k, v) for k, v in metric.iteritems() if not k.startswith('statsd.')):
	    for k, v in val.iteritems():
		    ck_metric = "metric  % 20s %s %f" % (name + '.' + k, 'float', v)
            	    ck_metrics.append(ck_metric)
		

def parse_file(file_path, offset=0):
    """
    Opens a metrics file from statsd and parses its json.

    Returns the offset of what we last read so we can seek
    directly to it next time.
    """
    with open(file_path, 'rb') as fd:
        fd.seek(offset)
        data = fd.read()
        for line in data.split("\n"):
            if line:
                output_metrics(json.loads(line))

        return fd.tell()

def find_latest_flush(files):
    s = sorted(files)
    currentFile = s.pop()
    for i in s:
        os.remove(i);
    return currentFile
        

def main():
    watch_dir = sys.argv[1]
    files = glob.glob(os.path.join(watch_dir, '[0-9]*.json'))
    output_check_status('200 OK')
    currentFile = find_latest_flush(files)
    relpath = os.path.relpath(currentFike, watch_dir)
    parse_file(file_path)
    
    """for file_path in files:
        relpath = os.path.relpath(file_path, watch_dir)
        state[relpath] = parse_file(file_path, state.get(relpath, 0))
    print('\n'.join(ck_metrics))
    update_state(watch_dir, state_file, state)
    """

if __name__ == "__main__":
    main()
