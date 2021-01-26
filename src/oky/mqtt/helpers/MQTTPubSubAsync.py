import paho.mqtt.client as mqttc
import time
from threading import Thread
from abc import ABC, abstractmethod


class Callbacks():
    def __init__(self, data_queue_out, logger=None, *args, **kwargs):
        self.data_queue_out = data_queue_out
        self.logger = logger
        super().__init__( *args, **kwargs)

    def on_message(self, client, userdata, message):
        self.data_queue_out.append(message)

    def register(self, client):
        client.on_message = self.on_message


class MQTTPubSubAsync(Thread, ABC):
    def __init__(
        self, 
        client_config, data_queue_in, data_queue_out, stop_event,
        topics=None, logger=None, 
        *args, **kwargs):
        self.client_config = client_config
        self.client = mqttc.Client(protocol=mqttc.MQTTv5)
        self.client.enable_logger(logger)
        self.stop_event = stop_event
        self.callbacks = Callbacks(data_queue_out)
        self.callbacks.register(self.client)
        self.data_queue_in = data_queue_in
        self.topics = topics
        self.logger = logger
        super().__init__(*args, **kwargs)
    
    def run(self):
        self.connect()
        self.client.loop_start()
        self.client.subscribe(self.topics)
        while not self.stop_event.is_set():
            try:
                data = self.data_queue_in.popleft()
                self.loop(data)
            except IndexError:
                time.sleep(0.5)
                continue
            except Exception as e:
                self.logger.fatal("Loop FATAL error: {}".format(e))
                break
        self.cleanup()        
    
    def connect(self):
        self.logger.info("Connecting. Client config: {}".format(self.name))
        self.client.connect_async(
            self.client_config["server"]["server_address"], 
            self.client_config["server"]["server_port"],
            self.client_config["server"]["keep_alive_time"]
        )
        self.logger.info("Client {} is connecting".format(self.name))
    
    def subscribe(self):
        self.client.subscribe()
    
    def publish(self, pub_value, pub_topic, pub_properties):
        info = self.client.publish(
            pub_topic, 
            pub_value, 
            2, 
            properties=pub_properties)
        if info.rc != mqttc.MQTT_ERR_SUCCESS:
            self.logger.error("Error sending message {}".format(info.rc))
    
    @abstractmethod
    def loop(self, data, time_check):
        pass
        
    def cleanup(self):
        self.client.unsubscribe(self.topics)
        self.client.loop_stop()
        self.client.disconnect()
        self.client.reinitialise()
        self.logger.info("Client {} stopped".format(self.name))
