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


PRIO_KEY = '/leader-election/priority'
VOTE_KEY = '/leader-election/vote'
LEADER_KEY = '/leader-election/leader'


class LeaderElectionParticipant(object):
    """Our participant model that is responsible for acting on events
    in happing in the cluster.

    """

    def __init__(self, name):
        self.name = name
        self._election_timeout = None
        self._start_election()

    def value_changed(self, peer, key, value):
        if key == '__heartbeat__':
            # We do not care about updates to the heartbeat value.
            return
        if key == VOTE_KEY:
            try:
                lvote = self.gossiper.get_local_value(VOTE_KEY)
                for peer in self.gossiper.live_peers():
                    rvote = self.gossiper.get_peer_value(peer, VOTE_KEY)
                    if rvote != lvote:
                        return
            except KeyError:
                return
            self.gossiper.set_local_state(LEADER_KEY, lvote)
        elif key == LEADER_KEY:
            try:
                lvote = self.gossiper.get_local_value(LEADER_KEY)
                for peer in self.gossiper.live_peers():
                    rvote = self.gossiper.get_peer_value(peer, LEADER_KEY)
                    if rvote != lvote:
                        return
            except KeyError:
                return
            self.leader_elected(lvote)

    def _vote(self):
        """Perform an election."""
        self._election_timeout = None
        suggested_peer = self.gossiper.name
        arrogance = self.gossiper.get_local_value(PRIO_KEY)
        for peer in self.gossiper.live_peers():
            p = self.gossiper.get_peer_value(peer, PRIO_KEY)
            if p > arrogance:
                suggested_peer = peer
                arrogance = p
        try:
            current_leader = self.gossiper.get_local_value(VOTE_KEY)
            if current_leader == suggested_peer:
                # No change.
                return
        except KeyError:
            pass
        self.gossiper.set_local_state(VOTE_KEY, suggested_peer)

    def _start_election(self):
        if self._election_timeout is not None:
            self._election_timeout.cancel()
        self._election_timeout = reactor.callLater(5, self._vote)

    def peer_alive(self, peer):
        """The gossip tells us that there's a new peer."""
        self._start_election()

    def peer_dead(self, peer):
        """The gossip tells us that there's a peer down."""
        self._start_election()

    def leader_elected(self, peer):
        """Notification about leader election results."""
        print self.name, "Leader elected:", peer


def runtest():
    CNT = 20
    print "Starting test with %d participants" % (CNT,)
    members = []
    for i in range(0, CNT):
        port = 9000 + i
        participant = LeaderElectionParticipant('127.0.0.1:%d' % (port,))
        gossiper = Gossiper(reactor, '127.0.0.1:%d' % (port), participant)
        gossiper.set_local_state(PRIO_KEY, i)
        reactor.listenUDP(port, gossiper)
        members.append(gossiper)
    for m in members[1:]:
        m.handle_new_peers([members[0].name])


if __name__ == '__main__':
    reactor.callLater(0, runtest)
    reactor.run()
