""" Module to load tweets for spark streaming access.  Modified only slightly
from this SO answer: http://stackoverflow.com/questions/27882631/consuming-twitter-stream-with-tweepy-and-serving-content-via-websocket-with-geve"""
from __future__ import absolute_import, print_function
import gevent
import gevent.monkey
gevent.monkey.patch_all()
from gevent.server import StreamServer

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from configparser import ConfigParser
from random import choice
import json
import os

CONFIG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                          '..', 'config/'))


class SparkStreamListener(StreamListener):
    """ Use twitter streaming API to stream to PySpark. """
    def __init__(self):
        config = ConfigParser()
        config.read(os.path.join(CONFIG_DIR, 'prod.cfg'))
        self.sockets = []
        auth = OAuthHandler(config.get('twitter', 'consumer_key'),
                            config.get('twitter', 'consumer_secret'))
        auth.set_access_token(config.get('twitter', 'access_token'),
                              config.get('twitter', 'access_token_secret'))
        self.stream = Stream(auth, self)

    def add_socket(self, ws):
        self.sockets.append(ws)
        print(self.sockets)

    def run(self):
        try:
            self.stream.filter(track=['python'])
        except Exception as e:
            print(e)
            self.stream.disconnect()

    def start(self):
        """ Start GEvent """
        gevent.spawn(self.run)

    def send(self, status):
        """ Send status to socket """
        print(self.sockets)
        if len(self.sockets) > 1:
            ws = choice(self.sockets)
        else:
            ws = self.sockets[0]
        try:
            ws.send(status.encode('utf-8'))
        except ValueError:
            print(e)
            # the web socket die..
            self.sockets.remove(ws)

    def on_data(self, data):
        decoded = json.loads(data)
        gevent.spawn(self.send, decoded.get('text') + '\n')
        return True

    def on_error(self, status):
        print("Error: %s", status)

    def on_timeout(self):
        print("tweepy timeout.. wait 30 seconds")
        gevent.sleep(30)


def app(socket, address):
    stream_listener = SparkStreamListener()
    stream_listener.start()
    stream_listener.add_socket(socket)
    while not socket.closed:
        gevent.sleep(0.1)

if __name__ == '__main__':
    server = StreamServer(('127.0.0.1', 9999), app)
    server.serve_forever()
