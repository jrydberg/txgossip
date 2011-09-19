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

from txgossip import recipies, gossip
from twisted.internet import reactor


class Participant(recipies.LeaderElectionMixin):

    def leader_elected(self, is_leader, peer):
        """Notification about leader election results."""
        print self._gossiper.name, "Leader elected:", peer


def runtest():
    CNT = 20
    print "Starting test with %d participants" % (CNT,)
    members = []
    for i in range(0, CNT):
        port = 9000 + i
        participant = Participant(reactor)
        gossiper = gossip.Gossiper(reactor, participant, address='127.0.0.1')
        gossiper[participant.PRIO_KEY] = i
        reactor.listenUDP(port, gossiper)
        members.append(gossiper)
    for m in members[1:]:
        m.seed([members[0].name])


if __name__ == '__main__':
    reactor.callLater(0, runtest)
    reactor.run()
