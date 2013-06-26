/*
 * Flush stats to disk for the rackspace monitoring agent.
 *
 * To enable this backend, include 'rackspace' in the backends
 * configuration array:
 *
 *   backends: ['rackspace']
 *
 * This backend supports the following config options:
 *
 *   flushInterval: The number of seconds between metric flushes
 *   outputDir: The directory to place metrics on disk
 */

var fs = require('fs');
var path = require('path');
var _ = require('underscore');

function RackspaceBackend(startupTime, config, stats){
  var self = this;

  this.lastFlush = startupTime;
  this.lastException = startupTime;
  this.config = config.rax || {};
  this.outputDir = path.normalize(this.config.outputDir) + '/';

  this.statsCache = {
    counters: {},
    timers: {},
    gauges: {}
  };

  stats.on('flush', self.flush.bind(self));
  stats.on('status', self.status.bind(self));
}

/**
  * Clear the metrics cache
  * @param {Metrics object} metrics Metrics provided by statsd.
  */
RackspaceBackend.prototype.clearMetrics = function(metrics) {
  var self = this;
/*
  _.each(self.statsCache, function(val, type) {
    if(!metrics[type]) {
      return;
    }

    _.each(metrics[type], function(val, name) {
      if(type == 'counters') {
        self.statsCache[type][name] = 0;
      } else if (type == 'timers') {
        self.statsCache[type][name] = null;
      }

    });
  });
  */
  self.statsCache = {
    counters: {},
    timers: {},
    gauges: {}
  };
};


RackspaceBackend.prototype.flush = function(timestamp, metrics) {
  var self = this,
      out;
  //console.dir(metrics);
  console.log('caching statsd metrics at', new Date(timestamp * 1000).toString());
  _.each(self.statsCache, function(metric,type) {
    
    if(!metrics[type]) return;

    _.each(metrics[type], function(metricType,name) {
      var value;

      if(type == 'timers')
      	if(metrics['timer_data'][name])
          value = createTimerObject(metrics['timer_data'][name]);
	else
	  return;
      else if (type == 'counters') {
        value = { rate: metrics['counter_rates'][name], i: 1.0 };
      }
      else if (type == 'gauges') {
        value = { value: metrics[type][name] };
      }

      if (!self.statsCache[type][name]) {
        self.statsCache[type][name] = value;
	return;
      }

      if(type == 'counters') {
	self.statsCache[type][name] = { 
	    rate: (self.statsCache[type][name].rate * self.statsCache[type][name].i + value.rate) / (self.statsCache[type][name].i + 1.0), 
	    i: self.statsCache[type][name].i + 1.0
	  } 
      } else if (type == 'timers') {
      	self.statsCache[type][name] = calculateTimerCache(self.statsCache[type][name], metrics['timer_data'][name]); 
      } else if (type == 'gauges') {
        self.statsCache[type][name] = { value: metrics[type][name] };
      }
    });
  });

  //console.dir(self.statsCache);
  if ((timestamp - this.lastFlush) < this.config.flushInterval) {
    return;
  }

  out = {
    counters: this.statsCache.counters,
    timers: this.statsCache.timers,
    gauges: this.statsCache.gauges,
  };

  _.each( out.counters, function (counter, counters, counterName) {
   delete counter.i;
  });

  delete out.counters['statsd.bad_lines_seen'];
  delete out.counters['statsd.packets_received'];

  var filename = this.outputDir + (new Date()).getTime().toString() + '.json';

  console.log('flushing stats to ' + filename);

  fs.writeFileSync(filename, JSON.stringify(out) + '\n');
  self.lastFlush = timestamp;
  self.clearMetrics(metrics);

};

RackspaceBackend.prototype.status = function(write) {
  ['lastFlush', 'lastException'].forEach(function(key) {
    write(null, 'console', key, this[key]);
  }, this);
};

exports.init = function(startupTime, config, events) {
  var instance = new RackspaceBackend(startupTime, config, events);
  return true;
};

function calculateTimerCache(cached,current) {
  var n = cached.count;
  return {
    mean_90: (cached.mean_90 * n + current.mean_90) / (n + 1),
    upper_90: Math.max(cached.upper_90, current.upper_90),
    upper: Math.max(cached.upper, current.upper),
    lower: Math.min(cached.lower, current.lower),
    count: cached.count + current.count,
    mean: (cached.mean * n + current.mean) / (n + 1)
    }
}

function createTimerObject(statsdTimer) {
  return {
    mean_90: statsdTimer.mean_90,
    upper_90: statsdTimer.upper_90,
    upper: statsdTimer.upper,
    lower: statsdTimer.lower,
    count: statsdTimer.count,
    mean: statsdTimer.mean
  }
}

