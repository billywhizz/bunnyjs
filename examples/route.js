var AMQPChannel = require("../lib/bunny").Channel;
var exchanges = ["oneflow.shipment.shipped", "oneflow.printer.create", "oneflow.order.create"];
var id = 0;
var queue = 0;
var publisher;
var config = {
  url: "amqp://admin:Tyeiow9288392@127.0.0.1/dev",
  prefetch: 100,
  reconnect: 1000
};

function createSubscriber(exchange) {
  var subscriber = new AMQPChannel(config);
  function onMessage(m, ack, mm) {
    console.dir(mm);
    ack(true);
  }
  function onSubscribed(err, subscription) {
    if(err) return console.error(err);
    console.dir(subscription);
  }
  function onOpen(err) {
    if(err) return console.error(err);
    subscriber.subscribe(exchange, "foo.bar", "oneflow.log.collect:" + (queue++), onSubscribed, onMessage);
  }
  subscriber.open(onOpen);
}
exchanges.forEach(createSubscriber);

publisher = new AMQPChannel(config);
function onPublisherOpen(err) {
  if(err) return console.error(err);
  function onPublish(err) {
    if(err) return console.error(err);
    setTimeout(next, 1000);
  }
  function next() {
    var exchange = exchanges.shift();
    exchanges.push(exchange);
    publisher.publish(exchange, {id: id++}, "foo.bar", onPublish);
  }
  next();
}
publisher.open(onPublisherOpen);
