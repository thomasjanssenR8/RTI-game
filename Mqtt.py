"""
The MQTT class is used as an interface in projects to the Paho MQTT class, with most relevant functions implemented.
"""

import logging
import paho.mqtt.client as mqtt
from typing import List, Union, Optional
import ssl

logger = logging.getLogger(__name__)


class Mqtt:
    def __init__(self, broker: str, port: int = 1883, subscription_topics: List[str] = None, mqtt_callback=None,
                 disconnect_callback=None, user: str = None, password: str = None, certificate: str = None):
        """Initialize the MQTT class by configuring some parameters in the Paho MQTT client

        :param broker: address of the MQTT broker
        :param port: port of the MQTT broker
        :param subscription_topics: topics to listen on
        :param mqtt_callback: Function to run when an MQTT message is received
        :param disconnect_callback: Function to run when MQTT connection is broken
        :param user: username to pub/sub on the broker
        :param password: password to pub/sub on the broker
        :param certificate: full path to the CA file (.crt) if SSL/TLS is required
        """
        self.broker = broker
        self.port = port
        self.mqtt_callback = mqtt_callback
        self.disconnect_callback = disconnect_callback
        self.connected_to_mqtt = False
        self.subscription_topics = subscription_topics if subscription_topics else []
        self.user = user
        self.password = password
        self.certificate = certificate
        self.mq = mqtt.Client()
        try:
            self.connect_mqtt()
        except:
            logger.warning("Can't connect to MQTT broker")

    def connect_mqtt(self):
        """Connect to the Paho MQTT client"""
        self.mq.on_connect = self.on_mqtt_connect
        self.mq.on_disconnect = self.on_mqtt_disconnect
        if self.user and self.password:
            self.mq.username_pw_set(self.user, self.password)
        if self.certificate:
            self.mq.tls_set(ca_certs=self.certificate, tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)
            self.mq.tls_insecure_set(True)
        try:
            self.mq.connect(self.broker, port=self.port)
            if len(self.subscription_topics) != 0 and self.mqtt_callback is not None:
                self.mq.on_message = self.on_mqtt_message
            self.mq.loop_start()
            while not self.connected_to_mqtt: pass  # busy wait until connected
        except:
            logger.warning("Failed to connect MQTT broker")
            self.connected_to_mqtt = False
            raise

    def on_mqtt_connect(self, client, userdata, flags_dict, rc):
        """Subscribe to given topics.

        :param client: unused
        :param userdata: unused
        :param flags_dict: unused
        :param rc: unused
        """
        self.connected_to_mqtt = True
        if len(self.subscription_topics) != 0:
            logger.debug(f"Subscribing to topics: {self.subscription_topics}")
            qos = 1
            subscriptions = [(topic, qos) for topic in self.subscription_topics]
            self.mq.subscribe(subscriptions)
        logger.debug(f"Connected to MQTT broker at {self.broker}")

    def on_mqtt_disconnect(self, client, userdata, rc):
        """Execute a function once the MQTT connection is broken.

        :param client: unused
        :param userdata: unused
        :param rc: unused
        """
        self.connected_to_mqtt = False
        logger.warning(f"MQTT broker {self.broker} disconnected")
        if self.disconnect_callback is not None:
            self.disconnect_callback()

    def on_mqtt_message(self, client, config, msg):
        """Execute a function once a message is received.

        :param client: the MQTT client that receives the message
        :param config: configuration of the MQTT broker
        :param msg: the received MQTT message
        """
        self.mqtt_callback(client, config, msg)

    def publish_message(self, topic, message):
        """Publish an MQTT message.

        :param topic: topic to publish on
        :param message: message to be published
        """
        self.mq.publish(topic, message)

    def disconnect(self):
        """Disconnect from the MQTT broker"""
        self.mq.loop_stop()
        self.mq.disconnect()
