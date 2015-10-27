var AMQPChannel = require("../lib/bunny").Channel;
var exchange = process.argv[2] || "oneflow.shipment.shipped";
var id = parseInt(process.argv[3] ||"0");
var publisher;
var config = {
  url: "amqp://admin:Tyeiow9288392@127.0.0.1/dev",
  prefetch: 100,
  reconnect: 1000
};

// one connection
var subscriber = new AMQPChannel(config);
// one channel with one consumer
subscriber.open(function(err) {
  if(err) return console.error(err);
  subscriber.subscribe(exchange, "foo.bar", "oneflow.log.collect:" + id, function(err, subscription) {
    if(err) return console.error(err);
  }, function(m, ack, message) {
    console.dir(message);
    if(message.fields.redelivered) {
      console.log("redeliver: true")
      // put it on the error queue
      ack(true);
    }
    else {
      console.log("original: false")
      ack(false, false, true);
    }
  });
});

// multiple consumers on one channel
subscriber.open(function(err) {
  if(err) return console.error(err);
  subscriber.subscribe(exchange, "foo.bar", "oneflow.log.collect:" + id, function(err, subscription) {
    if(err) return console.error(err);
  }, function(m, ack, message) {
    console.dir(message);
    if(message.fields.redelivered) {
      console.log("redeliver: true")
      // put it on the error queue
      ack(true);
    }
    else {
      console.log("original: false")
      ack(false, false, true);
    }
  });
  subscriber.subscribe(exchange, "foo.bar", "oneflow.log.collect:" + id, function(err, subscription) {
    if(err) return console.error(err);
  }, function(m, ack, message) {
    console.dir(message);
    if(message.fields.redelivered) {
      console.log("redeliver: true")
      // put it on the error queue
      ack(true);
    }
    else {
      console.log("original: false")
      ack(false, false, true);
    }
  });
});
