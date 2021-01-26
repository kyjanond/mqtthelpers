import paho.mqtt.client as mqttc
import time
from threading import Thread
from abc import ABC, abstractmethod

class MQTTPub(Thread,ABC):
    TIME_LIMIT = 600
    def __init__(self,client_config,data_queue,stop_event, logger, *args, **kwargs):
        self.data_queue = data_queue
        self.stop_event = stop_event
        self.client_config = client_config
        self.client = mqttc.Client(protocol=mqttc.MQTTv5)
        self.logger = logger
        self.client.enable_logger(logger)
        self.last_msg_time = time.time()

        super().__init__( *args, **kwargs)
    
    def run(self):
        self.connect()
        self.client.loop_start()
        while not self.stop_event.is_set():
            try:
                data = self.data_queue.popleft()
                time_check = data[0]>self.last_msg_time+self.TIME_LIMIT and self.TIME_LIMIT>0
                self.loop(data,time_check)
            except IndexError:
                time.sleep(0.1)
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
    
    def publish(self,pub_value,pub_topic):
        info = self.client.publish(pub_topic,pub_value,2)
        if info.rc != mqttc.MQTT_ERR_SUCCESS:
            self.logger.error("Error sending message {}".format(info.rc))
    
    @abstractmethod
    def loop(self,data,time_check):
        pass
        
    def cleanup(self):
        self.client.loop_stop()
        self.client.disconnect()
        self.client.reinitialise()
        self.logger.info("Client {} stopped".format(self.name))
