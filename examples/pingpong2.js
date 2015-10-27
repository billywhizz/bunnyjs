var AMQPChannel = require("../lib/bunny").Channel;
var exchange = process.argv[2] || "shipment.shipped";
var config = {
  url: "amqp://admin:Tyeiow9288392@127.0.0.1/dev",
  prefetch: 1000,
  reconnect: 1000
};
var send = 0;
var recv = 0;
var publish = 0;
var channel = new AMQPChannel(config);

function onPublish(err) {
  if(err) return console.error(err);
  send++;
}
function onMessage(m, ack, mm) {
  ack(true);
  recv++;
}
var b = new Buffer(10);
function onSubscribed(err, subscription) {
  if(err) return console.error(err);
  console.dir(subscription);
  function next() {
    channel.publish(exchange, b, "#", onPublish, {persistent: false});
    publish++;
    setTimeout(next,10);
  }
  next();
  var lasts = 0;
  var lastr = 0;
  var lastp = 0;
  setInterval(function() {
    console.log((send - lasts) + ":" + (recv - lastr) + ":" + (publish - lastp));
    lasts = send;
    lastr = recv;
    lastp = publish;
  }, 1000);
}
function onOpen(err) {
  if(err) return console.error(err);
  channel.subscribe(exchange, "#", "oneflow.log.collect", onSubscribed, onMessage);
}
channel.open(onOpen);
