import logging
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import time
from datetime import datetime

def get_utc_timestamp(timestamp):
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'


class UTCISOFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = time.gmtime(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%dT%H:%M:%S", ct)
            s = "%s.%03dZ" % (t, record.msecs)
        return s


class MQTTHandler(logging.Handler):
    DEFAULT_FMT = UTCISOFormatter(
        '{"timestamp": "%(asctime)s","levelno": %(levelno)s, "line":%(lineno)s, "message": "%(levelname)s: %(message)s" }'
    )
    DEFAULT_TOPIC_FMT = logging.Formatter(
        '%(processName)s/%(threadName)s/%(levelname)s'
    )
    """
    A handler class which writes logging records, appropriately formatted,
    to a MQTT server to a topic.
    """
    def __init__(self, hostname, topic, qos=0, retain=False,
            port=1883, client_id='', keepalive=60, will=None, auth=None,
            tls=None, protocol=mqtt.MQTTv31, transport='tcp'):
        logging.Handler.__init__(self)
        self.topic = topic
        self.qos = qos
        self.retain = retain
        self.hostname = hostname
        self.port = port
        self.client_id = client_id
        self.keepalive = keepalive
        self.will = will
        self.auth = auth
        self.tls = tls
        self.protocol = protocol
        self.transport = transport
        self.topic_formatter = None
    
    def setTopicFormatter(self, fmt):
        """
        Set the topic formatter for this handler.
        """
        self.topic_formatter = fmt
    
    def formatTopic(self, record):
        """
        Format the specified topic.

        If a formatter is set, use it. Otherwise, use the default formatter
        for the module.
        """
        if self.topic_formatter:
            fmt = self.topic_formatter
        else:
            fmt = self.DEFAULT_TOPIC_FMT
        return self.topic+"/"+fmt.format(record)
    
    def format(self, record):
        """
        Format the specified record.

        If a formatter is set, use it. Otherwise, use the default formatter.
        """
        if self.formatter:
            fmt = self.formatter
        else:
            fmt = self.DEFAULT_FMT
        return fmt.format(record)

    def emit(self, record):
        """
        Publish a single formatted logging record to a broker, then disconnect
        cleanly.
        """
        msg = self.format(record)
        topic = self.formatTopic(record)
        
        publish.single(topic, msg, self.qos, self.retain,
            hostname=self.hostname, port=self.port,
            client_id=self.client_id, keepalive=self.keepalive,
            will=self.will, auth=self.auth, tls=self.tls,
            protocol=self.protocol, transport=self.transport)

if __name__ == "__main__":
    hostname = '192.168.178.11'
    topic = 'logs'

    # Create MQTT handler
    myHandler = MQTTHandler(hostname, topic)
    myHandler.setLevel(logging.DEBUG)

    # Create and configure a logger instance
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(myHandler)

    # Create an MQTT client listen to new logs
    def on_message(client, userdata, message):
        print("Received message '" + str(message.payload) + "' on topic '"
            + message.topic + "' with QoS " + str(message.qos))
        print("---")
    client = mqtt.Client()
    client.connect(hostname)
    client.subscribe(topic+"/#")
    client.on_message = on_message

    # Listening
    client.loop_start()
    time.sleep(1)

    # Test logging from all log levels
    logger.debug("a message")
    logger.info("an message")
    logger.warning("a warning message")
    logger.error("an error message")
    logger.critical("a critical message")

    time.sleep(5)
    
    client.unsubscribe(topic+"/#")
    client.disconnect()
    client.loop_stop()