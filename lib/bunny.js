var amqp = require("amqplib/callback_api");

function AMQPChannel(config) {
  if (!(this instanceof AMQPChannel)) {
    return new AMQPChannel(config);
  }
  this.config = config;
  this.channel = null;
  this.connection = null;
}

AMQPChannel.prototype.close = function(cb) {
  if(this.channel) {
    this.channel.close(cb);
  }
};

var ConnectionPool = new function() {
  var connections = {};
  this.acquire = function(url, cb) {
    if(connections[url]) {
      if(connections[url].connection) {
        cb(null, connections[url].connection);
      }
      else {
        connections[url].pending.push(cb);
      }
      return;
    }
    connections[url] = {
      pending: [cb]
    };
    amqp.connect(url, function(err, conn) {
      if(err) {
        while(connections[url].pending.length) {
          connections[url].pending.shift()(err, conn);
        }
        delete connections[url];
        return;
      }
      conn.setMaxListeners(500);
      conn.on("error", function(err) {
        console.error(err.toString());
      });
      conn.once("close", function() {
        delete connections[url];
      });
      connections[url].connection = conn;
      while(connections[url].pending.length) {
        connections[url].pending.shift()(null, conn);
      }
    });
  }
}();

AMQPChannel.prototype.open = function(cb) {
  if(this.channel) {
    return cb();
  }
  var _broker = this;
  var config = this.config;
  function channelHandler(err, ch) {
    if(err) return cb(err);
    _broker.channel = ch;
    ch.once("close", function() {
      _broker.channel = null;
      if(_broker.onClose) _broker.onClose();
    });
    cb();
  }
  if(this.connection) {
    this.connection.createConfirmChannel(channelHandler);
    return;
  }
  ConnectionPool.acquire(config.url, function(err, conn) {
    if(err) {
      console.error(err);
      setTimeout(function() {
        _broker.open(cb);
      }, config.reconnect);
      return;
    }
    conn.once("close", function() {
      _broker.channel = null;
      _broker.connection = null;
      setTimeout(function() {
        _broker.open(cb);
      }, config.reconnect);
    });
    _broker.connection = conn;
    conn.createConfirmChannel(channelHandler);
  });
  return _broker;
};

AMQPChannel.prototype.subscribe = function(exchange, topic, queue, cb, mcb, queueOptions) {
  if(!this.channel) {
    cb(new Error("no channel"));
    return;
  }
  var _broker = this;
  var config = this.config;
  var channel = this.channel;
  if(!queueOptions) {
    queueOptions = {};
  }
  queueOptions.contentType = "application/json";
  if(!queueOptions.hasOwnProperty("persistent")) queueOptions.persistent = true;
  if(!queueOptions.hasOwnProperty("autoDelete")) queueOptions.autoDelete = true;
  channel.assertExchange(exchange, "topic", config.exchangeOptions, function(err) {
    if(err) return cb(err);
    channel.assertQueue(queue, queueOptions, function(err, q) {
      if(err) return cb(err);
      channel.prefetch(config.prefetch);
      channel.bindQueue(queue, exchange, topic, {}, function(err) {
        if(err) return cb(err);
        var consumerTag = channel.consume(queue, function(msg) {
          if (msg !== null) {
            try {
              //res = mcb(JSON.parse(msg.content.toString()), function(ack) {  
              res = mcb(msg.content, function(ack, allUpTo, requeue) {  
                if(ack) return channel.ack(msg, allUpTo);
                channel.nack(msg, allUpTo, requeue);
              }, msg);
            } catch(ex) {
              channel.nack(msg);
            };
          }
          else {
            //TODO
          }
        }, null, function(err, subscription) {
          if(err) return cb(err);
          _broker.consumerTag = subscription.consumerTag;
          cb(null, subscription);
        });
      });
    })
  });
  return _broker;
};

AMQPChannel.prototype.unsubscribe = function(cb) {
  if(!this.channel) {
    cb(new Error("no channel"));
    return;
  }
  this.channel.cancel(this.consumerTag, cb);
};

AMQPChannel.prototype.close = function(cb) {
  if(!this.channel) {
    cb(new Error("no channel"));
    return;
  }
  this.channel.close(cb);
};

AMQPChannel.prototype.publish = function(exchange, msg, topic, cb, messageOptions) {
  if(!this.channel) {
    cb(new Error("no channel"));
    return;
  }
  var _broker = this;
  var config = this.config;
  var channel = this.channel
  if(!messageOptions) messageOptions = {};
  if(!messageOptions.hasOwnProperty("persistent")) messageOptions.persistent = true;
  if(!messageOptions.hasOwnProperty("mandatory")) messageOptions.mandatory = true;
  if(!messageOptions.hasOwnProperty("type")) messageOptions.type = topic;
  if(!messageOptions.hasOwnProperty("contentType")) messageOptions.contentType = "application/json";
  function tryPublish() {
    //var written = channel.publish(exchange, topic, new Buffer(JSON.stringify(msg)), messageOptions, function(err) {
    var written = channel.publish(exchange, topic, msg, messageOptions, function(err) {
      cb(err);
    });
    if(!written) {
      channel.once("drain", function() {
        console.log("channel.drain");
        tryPublish();
      });
      return;
    }
  }
  channel.assertExchange(exchange, "topic", config.exchangeOptions, function(err) {
    if(err) return cb(err);
    tryPublish();
  });
  return _broker;
};

exports.Channel = AMQPChannel;