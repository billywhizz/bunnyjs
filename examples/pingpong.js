var AMQPChannel = require("../lib/bunny").Channel;
var id = 0;
var config = {
  exchange: "oneflow.log.event",
  url: "amqp://oneflow/dev",
  prefetch: 100,
  reconnect: 1000
};
var subscriber = new AMQPChannel(config);
subscriber.open(function(err) {
  if(err) return console.error(err);
  subscriber.subscribe("#", "oneflow.log.swarm", function(err, subscription) {
    if(err) return console.error(err);
  }, function(m, ack) {
    if(m.id !== id +1) throw new Error("out of bounds");
    id = m.id;
    ack(true);
  });
});
var publisher = new AMQPChannel(config);
publisher.open(function(err) {
  if(err) return console.error(err);
  function next() {
    publisher.publish({
      id: id
    }, "", function(err) {
      if(err) return console.error(err);
      id++;
      setImmediate(next);
    }, {
      persistent: true
    })
  }
  next();
});
setInterval(function() {
  console.log(id);
}, 1000);
