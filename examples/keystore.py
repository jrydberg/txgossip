# Copyright (C) 2011 Johan Rydberg
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


from txgossip.gossip import Gossiper
from twisted.internet import reactor
import random


class KeyStoreParticipant(object):
    """Example showing a simple key-value store with eventual
    consistency.

    Each value is annotated with a timestamp.  When there's two
    conflicting changes, the one with the latest timestamp wins.

    The implementation expects all participants to have synced clocks.
    """

    def __init__(self, clock, storage):
        self.clock = clock
        self.storage = storage

    def value_changed(self, peer, key, timestamp_value):
        if key == '__heartbeat__':
            # We do not care about updates to the heartbeat value.
            return
        print self.gossiper.name, "got", key
        timestamp, value = timestamp_value
        if key in self.storage:
            current_timestamp, current_value = self.storage[key]
            if timestamp <= current_timestamp:
                return
        self.storage[key] = (timestamp, value)

    def __setitem__(self, key, value):
        self.gossiper.set_local_state(key,
            (self.clock.seconds(), value))

    def __getitem__(self, key):
        return self.storage[key][1]

    def peer_alive(self, peer):
        """The gossip tells us that there's a new peer."""
        for key in self.gossiper.get_peer_keys(peer):
            self.value_changed(peer, key, self.gossiper.get_peer_value(
                    peer, key))

    def peer_dead(self, peer):
        """The gossip tells us that there's a peer down."""



def get_value(members):
    participant = random.choice(members)
    print "Got value", participant['test-key']

def set_value(members):
    print "Set value"
    participant = random.choice(members)
    participant['test-key'] = 'test-value'
    reactor.callLater(8, get_value, members)


def runtest():
    CNT = 20
    print "Starting test with %d participants" % (CNT,)
    members = []
    peers = []
    for i in range(0, CNT):
        port = 9000 + i
        participant = KeyStoreParticipant(reactor, {})
        gossiper = Gossiper(reactor, '127.0.0.1:%d' % (port), participant)
        reactor.listenUDP(port, gossiper)
        members.append(participant)
        peers.append(gossiper)
    for m in peers[1:]:
        m.handle_new_peers([peers[0].name])
    reactor.callLater(10, set_value, members)

if __name__ == '__main__':
    reactor.callLater(0, runtest)
    reactor.run()
