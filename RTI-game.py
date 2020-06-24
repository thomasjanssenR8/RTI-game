"""
Date created: 24/06/2020
Author: Thomas Janssen
Project: RTI-game

STEM Zomerkamp project: Handle inputs from an RTI environment to control a web-based game.
"""

from Mqtt import Mqtt
import logging
import platform
from time import sleep
import signal
import json
import numpy as np
import matplotlib.pyplot as plt


class Game:
    def __init__(self):
        """
        Init the connection to the MQTT broker.
        If you have troubles, install mosquitto and try to publish and subscribe to a topic locally:
        mosquitto_sub -h localhost -t "data"
        mosquitto_pub -h localhost -t "data" -m "hello world"
        """
        self.mqtt_client = Mqtt(broker='localhost', port=1883, user=None, password=None, subscription_topics=['data'],
                                mqtt_callback=self.msg_callback)

    def run(self):
        keep_running = True
        logger.info('Game started.')
        while keep_running:
            try:
                if platform.system() == "Windows":
                    sleep(1)
                else:
                    signal.pause()
            except KeyboardInterrupt:
                logger.warning("received KeyboardInterrupt... stopping")
                keep_running = False

    def msg_callback(self, client, config, msg):
        try:
            message = json.loads(msg.payload)
            logger.info(f'Message received: {message}')
            matrix = message['tolist']


            plt.imshow(matrix)
            plt.colorbar()
            plt.show()
        except Exception as e:
            logger.error(e)


# Create global logger
logger = logging.getLogger(__name__)
formatstring = "%(asctime)s - %(name)s:%(funcName)s:%(lineno)i - %(levelname)s - %(message)s"
logging.basicConfig(format=formatstring, level=logging.DEBUG)

# Start the game
Game().run()

