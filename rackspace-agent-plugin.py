#!/usr/bin/env python

import os.path
import sys
import glob
import json

ck_metrics = []

METRIC_TYPE_MAP = {
    'counters': 'float',
}


def load_state(state_file):
    """
    Load state from '.state.json'.
    The state file contains the offset location of the last place we read
    the file at in the format of:
        {
            'filename': offset,
            'filename2': offset
        }
    """
    try:
        fd = open(state_file, 'rb')
    except IOError as e:
        if e.errno == 2:
            return {}
        raise
    else:
        data = fd.read()
        return json.loads(data)


def update_state(watch_dir, state_file, state):
    """
    Delete all old files (any files older than the latest file) and write new
    state to disk.
    """
    timestamps = sorted([int(x.split('.json')[0]) for x in state.keys()])
    timestamps.pop()

    for ts in timestamps:
        ts_file = "%s.json" % ts
        try:
            os.remove(os.path.join(watch_dir, ts_file))
        except OSError, e:
	    
            """if e.errno == 2:
                print "Unable to remove %s" % ts_file
            else:
                output_check_status("err", e.strerror)
	    """

        del state[ts_file]

    with open(state_file, 'wb') as fd:
        fd.write(json.dumps(state))


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


def main():
    watch_dir = sys.argv[1]
    state_file = os.path.join(watch_dir, '.state.json')
    state = load_state(state_file)
    files = glob.glob(os.path.join(watch_dir, '[0-9]*.json'))
    output_check_status('200 OK')
    for file_path in files:
        relpath = os.path.relpath(file_path, watch_dir)
        state[relpath] = parse_file(file_path, state.get(relpath, 0))
    print('\n'.join(ck_metrics))
    update_state(watch_dir, state_file, state)


if __name__ == "__main__":
    main()
