import paho.mqtt.client as mqttc
from paho.mqtt.subscribeoptions import SubscribeOptions
import time
from threading import Thread, Event
from collections import deque
from datetime import datetime


def build_status_msg(status, status_topic):
    msg = mqttc.MQTTMessage(topic=str.encode(status_topic))
    msg.payload = status
    msg.qos = 1
    msg.retain = True
    msg.properties = None
    return msg

class Callbacks():
    def __init__(self, onmessage_queue, publish_queue, is_connected, status_topic=None, logger=None, *args, **kwargs):
        self.is_connected = is_connected
        self.onmessage_queue = onmessage_queue
        self.publish_queue = publish_queue
        self.status_topic = status_topic
        self.logger = logger
        super().__init__(*args, **kwargs)

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            client.connected_flag = True
            self.is_connected.set()
            self.logger.info("Client {} connection: SUCCESS".format(client._client_id))
            if self.status_topic: self.publish_queue.append(build_status_msg(1,self.status_topic))
        else:
            client.connected_flag = False
            self.is_connected.clear()
            self.logger.error("Client {} connection: FAIL {}".format(client._client_id, rc))
            if self.status_topic: self.publish_queue.append(build_status_msg(0,self.status_topic))
    
    def on_disconnect(self, client, userdata, rc, properties=None):
        client.connected_flag = False
        self.is_connected.clear()
        if rc == 0:
            self.logger.info("Client {} is disconnected.".format(client._client_id))
        else:
            self.logger.error("Client {} is disconnected: {}.".format(client._client_id,rc))

    def on_message(self, client, userdata, message):
        self.onmessage_queue.append(message)
        self.logger.info("Msg received topic: {}, payload: {}".format(message.topic, message.payload))

    def register(self, client):
        client.on_message = self.on_message
        client.on_connect = self.on_connect
        client.on_disconnect = self.on_disconnect


class DummyLogger:
    def debug(self,msg):
        pass

    def info(self,msg):
        pass
    
    def error(self,msg):
        pass
    
    def warning(self,msg):
        pass
    
    def fatal(self,msg):
        pass


class MQTTPubSub(Thread):
    def __init__(
            self,
            client_config, stop_event,
            status_topic=None, sub_topics=None, logger=None, name="mqtt_pubsub",
            *args, **kwargs):
        if logger is None:
            logger = DummyLogger()
        self.status_topic = status_topic
        self.client_config = client_config
        self.publish_queue = deque()
        self.onmessage_queue = deque()
        self.stop_event = stop_event
        self.is_connected = Event()

        self.client = mqttc.Client(protocol=mqttc.MQTTv5)
        self.client.enable_logger(logger)

        self.callbacks = Callbacks(
            self.onmessage_queue, 
            self.publish_queue,
            self.is_connected,
            status_topic=self.status_topic,
            logger=logger)
        self.callbacks.register(self.client)

        self.sub_topics = sub_topics
        self.logger = logger
        super().__init__(name=name, *args, **kwargs)

    def run(self):
        self._connect()
        self.client.loop_start()
        ret = 0
        while not self.stop_event.is_set() and ret>=0:
            ret = self._loop()
        self._cleanup()
    
    def publish_msg(self, mqtt_msg):
        self.publish_queue.append(mqtt_msg)
    
    def get_msg(self):
        try:
            return self.onmessage_queue.popleft()
        except IndexError:
            return None

    def _subscribe(self):
        self.logger.info(self.sub_topics)
        if self.sub_topics is not None:
            self.client.subscribe(self.sub_topics,options=SubscribeOptions(1, noLocal=True))

    def _connect(self):
        self.logger.info("Connecting. Client config: {}".format(self.name))
        self.client.connect_async(
            self.client_config["server"]["server_address"], 
            self.client_config["server"]["server_port"],
            self.client_config["server"]["keep_alive_time"]
        )
        self.logger.info("Client {} is connecting".format(self.name))
    
    def _set_will(self):
        if self.status_topic:
            self.client.will_set(
                self.status_topic,
                0,
                1,
                True
            )
    
    def _handle_message(self, mqtt_msg):
        if type(mqtt_msg) != mqttc.MQTTMessage:
            self.logger.error("Not an MQTTMessage: {}".format(type(mqtt_msg)))
            return 100
        if mqtt_msg.topic == self.status_topic and self.client.connected_flag:
            self.logger.info("Client connected. Subscribing...")
            self._subscribe()
            self.is_connected = True
        ret = self._publish(mqtt_msg)
        if ret == 0:
            self.logger.debug("Publish successful")
        return ret

    def _publish(self, mqtt_msg):
        info = self.client.publish(
            mqtt_msg.topic,
            mqtt_msg.payload,
            mqtt_msg.qos,
            mqtt_msg.retain,
            mqtt_msg.properties)
        if info.rc != mqttc.MQTT_ERR_SUCCESS:
            self.logger.error("Error sending message {}".format(info.rc))
            return info.rc
        else:
            return 0
    
    def _loop(self):
        try:
            ret = self._handle_message(self.publish_queue.popleft())
        except IndexError:
            time.sleep(0.1)
            ret = 1
        except Exception as e:
            self.logger.fatal("Loop FATAL error: {}".format(e))
            ret = -1
        return ret

    def _cleanup(self):
        if self.status_topic:
            self._publish(build_status_msg(0,self.status_topic))
        self.logger.info("Client disconnecting. Un-subscribing...")
        if self.sub_topics is not None:
            self.client.unsubscribe([x[0] for x in self.sub_topics])
        self.client.loop_stop()
        self.client.disconnect()
        self.client.reinitialise()
        self.logger.info("Client {} stopped".format(self.name))
