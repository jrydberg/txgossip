# Copyright (C) 2011 Johan Rydberg
# Copyright (C) 2010 Bob Potter
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import random
import json

from txgossip.state import PeerState
from txgossip.scuttle import Scuttle
from twisted.python import log
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import task


def _address_from_peer_name(name):
    address, port = name.split(':', 1)
    return address, int(port)

def _address_to_peer_name(address):
    return '%s:%d' % (address.host, address.port)


class Gossiper(DatagramProtocol):

    def __init__(self, clock, name, participant):
        self.state = PeerState(clock, name, participant)
        self.peers = {}
        self.name = name
        self.scuttle = Scuttle(self.peers, self.state)
        self._heart_beat_timer = task.LoopingCall(self.beat_heart)
        self._heart_beat_timer.clock = clock
        self._gossip_timer = task.LoopingCall(self.gossip)
        self._gossip_timer.clock = clock
        self.clock = clock
        self.participant = participant
        self.participant.gossiper = self

    def handle_new_peers(self, seeds):
        #print self.name, "HANDLE NEW", seeds
        for peer_name in seeds:
            if peer_name in self.peers:
                continue
            self.peers[peer_name] = PeerState(self.clock,
                 peer_name, self.participant)

    def startProtocol(self):
        self._heart_beat_timer.start(1, now=True)
        self._gossip_timer.start(1, now=True)
        self.peers[self.name] = self.state

    def stopProtocol(self):
        self._gossip_timer.stop()
        self._heart_beat_timer.stop()

    def beat_heart(self):
        self.state.beat_that_heart()

    def datagramReceived(self, data, address):
        self.handle_message(json.loads(data), address)

    def gossip(self):
        live_peers = self.live_peers()
        dead_peers = self.dead_peers()
        if live_peers:
            self.gossip_with_peer(random.choice(live_peers))

        prob = len(dead_peers) / float(len(live_peers) + 1)
        if random.random() < prob:
            self.gossip_with_peer(random.choice(dead_peers))

        for state in self.peers.values():
            state.check_suspected()

    def request_message(self):
        return json.dumps({
            'type': 'request', 'digest': self.scuttle.digest()
            })

    def gossip_with_peer(self, peer):
        #print self.name, "will gossip with", peer
        self.transport.write(self.request_message(),
            _address_from_peer_name(peer))

    def handle_message(self, message, address):
        """Handle an incoming message."""
        if message['type'] == 'request':
            self.handle_request(message, address)
        elif message['type'] == 'first-response':
            self.handle_first_response(message, address)
        elif message['type'] == 'second-response':
            self.handle_second_response(message, address)

    def handle_request(self, message, address):
        deltas, requests, new_peers = self.scuttle.scuttle(
            message['digest'])
        #print self.name, "got a request", deltas, requests, new_peers
        self.handle_new_peers(new_peers)
        # Send a first response to the one requesting our data.
        response = json.dumps({
            'type': 'first-response', 'digest': requests, 'updates': deltas
            })
        self.transport.write(response, address)

    def handle_first_response(self, message, address):
        self.scuttle.update_known_state(message['updates'])
        response = json.dumps({
            'type': 'second-response',
            'updates': self.scuttle.fetch_deltas(
                    message['digest'])
            })
        self.transport.write(response, address)

    def handle_second_response(self, message, address):
        self.scuttle.update_known_state(message['updates'])

    def set_local_state(self, key, value):
        self.state.update_local(key, value)

    def get_local_value(self, key):
        return self.state.attrs[key][0]

    def keys(self):
        return self.state.attrs.keys()

    def get_peer_value(self, peer, key):
        return self.peers[peer].attrs[key][0]

    def get_peer_keys(self, peer):
        return self.peers[peer].attrs.keys()

    def live_peers(self):
        return [n for (n, p) in self.peers.items()
                if p.alive and n != self.name]

    def dead_peers(self):
        return [n for (n, p) in self.peers.items()
                if not p.alive and n != self.name]

