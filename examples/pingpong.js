var AMQPChannel = require("../lib/bunny").Channel;
var config = {
  exchange: "oneflow.log.event",
  url: "amqp://admin:Tyeiow9288392@127.0.0.1/dev",
  prefetch: 100,
  reconnect: 1000
};
var id = 0;
function client() {
  var subscriber = new AMQPChannel(config);
  var publisher = new AMQPChannel(config);
  subscriber.open(function(err) {
    if(err) return console.error(err);
    subscriber.subscribe(config.exchange, "#", "oneflow.log.swarm", function(err, subscription) {
      if(err) return console.error(err);
      publisher.open(function(err) {
        if(err) return console.error(err);
        publisher.publish(config.exchange, {id: id}, "", function(err) {
          if(err) return console.error(err);
        });
      });
    }, function(m, ack) {
      m.id = id++;
      publisher.publish(config.exchange, m, "", function(err) {
        if(err) return console.error(err);
      });
      ack(true);
    });
  });
}
setInterval(function() {
  console.log(id);
}, 1000);
client();
