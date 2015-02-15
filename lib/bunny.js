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
      conn.on("close", function() {
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
    ch.on("close", function() {
      _broker.channel = null;
      if(_broker.onClose) _broker.onClose();
    });
    if(config.exchange) {
      ch.assertExchange(config.exchange, "topic", config.exchangeOptions, cb);
      return;
    }
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
    conn.on("error", function(err) {
      console.error(err.toString());
    });
    conn.on("close", function() {
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

AMQPChannel.prototype.subscribe = function(topic, queue, cb, mcb, queueOptions) {
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
  channel.assertQueue(queue, queueOptions, function(err, q) {
    if(err) return cb(err);
    channel.prefetch(config.prefetch);
    channel.bindQueue(queue, config.exchange, topic, {}, function(err) {
      if(err) return cb(err);
      var consumerTag = channel.consume(queue, function(msg) {
        if (msg !== null) {
          try {
            res = mcb(JSON.parse(msg.content.toString()), function(ack) {  
              if(ack) return channel.ack(msg);
              channel.nack(msg);
            });
          } catch(ex) {
            channel.nack(msg);
          };
        }
        else {
          //TODO
        }
      }, null, function(err, ok) {
        if(err) return cb(err);
        _broker.consumerTag = ok.consumerTag;
        console.dir(ok);
      });
    });
  })
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

AMQPChannel.prototype.publish = function(msg, topic, cb, messageOptions) {
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
  messageOptions.contentType = "application/json";
  function tryPublish() {
    var written = channel.publish(config.exchange, topic, new Buffer(JSON.stringify(msg)), messageOptions, function(err) {
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
  tryPublish();
  return _broker;
};

exports.Channel = AMQPChannel;