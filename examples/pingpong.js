var AMQPChannel = require("../lib/bunny").Channel;
var config = {
  exchange: "oneflow.log.event",
  url: "amqp://oneflow/dev",
  prefetch: 100,
  reconnect: 1000
};
var id = 0;
function client() {
  var subscriber = new AMQPChannel(config);
  var publisher = new AMQPChannel(config);
  subscriber.open(function(err) {
    if(err) return console.error(err);
    subscriber.subscribe("#", "oneflow.log.swarm", function(err, subscription) {
      if(err) return console.error(err);
      publisher.open(function(err) {
        if(err) return console.error(err);
        publisher.publish({id: id}, "", function(err) {
          if(err) return console.error(err);
        });
      });
    }, function(m, ack) {
      m.id = id++;
      publisher.publish(m, "", function(err) {
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
client();
client();
client();
client();
client();
client();
client();
client();
client();