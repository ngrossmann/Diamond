# vim: expandtab shiftwidth=4 softtabstop=4 smarttab
# coding=utf-8

"""
Send collected stats to [Kafka](http://kafka.apache.org/) fromated as json
or [graphite like](https://graphite.readthedocs.org/en/latest/feeding-carbon.html)
plain text.

Dependency:

- [kafka-python](https://github.com/mumrah/kafka-python) (pip install kafka-python)

Configuration:

```
handlers = diamond.handler.kafka.KafkaHandler

[[KafkaHandler]]
host = localhost
port = 9092
topic = diamond
```
"""

from __future__ import absolute_import
from diamond.handler.Handler import Handler
import json
from kafka import KafkaClient, SimpleProducer


class KafkaHandler(Handler):
    """
    Send metrics to kafka topic.
    """
    def __init__(self, config = None):
        super(KafkaHandler, self).__init__(config)
        fmt = self.config['format']
        if fmt == 'json':
            self.format = lambda metric: JsonMetric(metric).json()
        elif fmt == 'graphite':
            self.format = lambda metric: "%s %s %s" % (metric.path,
                    metric.value, metric.timestamp)
        else:
            self.log.error("Unsupported format `%s', using json", self.format)
            self.format = lambda metric: JsonMetric(metric).json()
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.kafka = KafkaClient('%s:%d' % (self.host, self.port))
        self.producer = SimpleProducer(self.kafka)
        self.topic = self.config['topic']
        self.log.info('Connected to Kafka at %s:%d', self.host, self.port)

    def process(self, metric):
        """
        Send metric to Kafka.
        Process a metric by doing nothing
        """
        try:
            self.producer.send_messages(self.topic, self.format(metric))
        except:
            self.log.exception('Faild to send metric to Kafka topic %s at %s %d', 
                    self.topic, self.host, self.port)

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(KafkaHandler, self).get_default_config_help()

        config.update({
            'format': 'Metric format (values: json, graphite)',
            'host': 'Name of kafka server',
            'port': 'Kafka port',
            'topic': 'Kafka topic to publish metrics'
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(KafkaHandler, self).get_default_config()

        config.update({
            'format': 'json',
            'host': 'localhost',
            'port': '9092',
            'topic': 'diamond'
        })

        return config

class JsonMetric(object):
    def __init__(self, metric):
        self.timestamp = metric.timestamp
        self.value = metric.value
        self.tags = dict()
        self.tags['server'] = metric.host
        self.tags['target_type'] = metric.metric_type.lower()
        self.tags['what'] = metric.getCollectorPath()
        self.tags['path'] = metric.path
        self.tags['type'] = metric.getMetricPath()

    def __repr__(self):
        tags = ""
        for key in self.tags.keys():
            tags = tags + "%s=%s " % (key, self.tags[key])
        return "%s %s %d" % (tags, self.value, self.timestamp)

    def json(self):
        return json.dumps(self.__dict__)
