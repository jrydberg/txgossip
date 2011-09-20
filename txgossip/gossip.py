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


class Participant(object):
    """Base class for participants."""

    def make_connection(self, gossiper):
        """Attach this participant to a gossiper."""
        self.gossiper = gossiper

    def peer_alive(self, peer_name):
        """Report that there's a peer alive."""

    def peer_dead(self, peer_name):
        """Report that there's a peer dead."""


class Gossiper(DatagramProtocol):

    def __init__(self, clock, participant, address=None):
        """Create a new gossiper.

        @param address: Listen address if the gossiper will not be
            bound to a specific listen interface.
        @param address: C{str}
        """
        self.state = PeerState(clock, participant)
        self._states = {}
        self._address = address
        self._scuttle = Scuttle(self._states, self.state)
        self._heart_beat_timer = task.LoopingCall(self._beat_heart)
        self._heart_beat_timer.clock = clock
        self._gossip_timer = task.LoopingCall(self._gossip)
        self._gossip_timer.clock = clock
        self.clock = clock
        self.participant = participant
        self._seeds = []

    def _setup_state_for_peer(self, peer_name):
        """Setup state for a new peer."""
        self._states[peer_name] = PeerState(self.clock, self.participant,
            name=peer_name)

    def seed(self, seeds):
        """Tell this gossiper that there are gossipers to
        be found at the given endpoints.

        @param seeds: a sequence of C{'ADDRESS:PORT'} strings.
        """
        self._seeds.extend(seeds)
        self._handle_new_peers(seeds)

    def _handle_new_peers(self, names):
        """Set up state for new peers."""
        for peer_name in names:
            if peer_name in self._states:
                continue
            self._setup_state_for_peer(peer_name)

    def _determine_endpoint(self):
        """Determine the IP address of this peer.

        @raises Exception: If it is not impossible to figure out the
            address.
        @return: a C{ADDRESS:PORT} string.
        """
        # Figure our our endpoint:
        host = self.transport.getHost()
        if not self._address:
            self._address = host.host
            if self._address == '0.0.0.0':
                raise Exception("address not specified")
        return '%s:%d' % (self._address, host.port)

    def startProtocol(self):
        """Start protocol."""
        self.name = self._determine_endpoint()
        self.state.set_name(self.name)
        self._states[self.name] = self.state
        self._heart_beat_timer.start(1, now=True)
        self._gossip_timer.start(1, now=True)
        self.participant.make_connection(self)

    def stopProtocol(self):
        """Stop protocol."""
        self._gossip_timer.stop()
        self._heart_beat_timer.stop()

    def _beat_heart(self):
        """Beat heart of our own state."""
        self.state.beat_that_heart()

    def datagramReceived(self, data, address):
        """Handle a received datagram."""
        self._handle_message(json.loads(data), address)

    def _gossip(self):
        """Initiate a round of gossiping."""
        live_peers = self.live_peers
        dead_peers = self.dead_peers
        if live_peers:
            self._gossip_with_peer(random.choice(live_peers))

        prob = len(dead_peers) / float(len(live_peers) + 1)
        if random.random() < prob:
            self._gossip_with_peer(random.choice(dead_peers))

        for state in self._states.values():
            if state.name != self.name:
                state.check_suspected()

    def _gossip_with_peer(self, peer):
        """Send a gossip message to C{peer}."""
        self.transport.write(json.dumps({
            'type': 'request', 'digest': self._scuttle.digest()
            }), _address_from_peer_name(peer.name))

    def _handle_message(self, message, address):
        """Handle an incoming message."""
        if message['type'] == 'request':
            self._handle_request(message, address)
        elif message['type'] == 'first-response':
            self._handle_first_response(message, address)
        elif message['type'] == 'second-response':
            self._handle_second_response(message, address)

    def _handle_request(self, message, address):
        """Handle an incoming gossip request."""
        deltas, requests, new_peers = self._scuttle.scuttle(
            message['digest'])
        self._handle_new_peers(new_peers)
        response = json.dumps({
            'type': 'first-response', 'digest': requests, 'updates': deltas
            })
        self.transport.write(response, address)

    def _handle_first_response(self, message, address):
        """Handle the response to a request."""
        self._scuttle.update_known_state(message['updates'])
        response = json.dumps({
            'type': 'second-response',
            'updates': self._scuttle.fetch_deltas(
                    message['digest'])
            })
        self.transport.write(response, address)

    def _handle_second_response(self, message, address):
        """Handle the ack of the response."""
        self._scuttle.update_known_state(message['updates'])

    def live_peers():
        """Property for all peers that we know is alive.

        The property holds a sequence L{PeerState}'s.
        """
        def get(self):
            return [p for (n, p) in self._states.items()
               if p.alive and n != self.name]
        return get,
    live_peers = property(*live_peers())

    def dead_peers():
        """Property for all peers that we know is dead.

        The property holds a sequence L{PeerState}'s.
        """
        def get(self):
            return [p for (n, p) in self._states.items()
               if not p.alive and n != self.name]
        return get,
    dead_peers = property(*dead_peers())

    # dict-like interface:

    def __getitem__(self, key):
        return self.state[key]

    def set(self, key, value):
        self.state[key] = value

    def __setitem__(self, key, value):
        self.set(key, value)

    def __contains__(self, key):
        return key in self.state

    def has_key(self, key):
        return key in self.state

    def __len__(self):
        return len(self.state)

    def __iter__(self):
        return iter(self.state)

    def keys(self):
        return self.state.keys()

    def get(self, key, default=None):
        if key in self.state:
            return self.state[key]
        return default
