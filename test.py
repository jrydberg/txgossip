from txgossip.gossip import Gossiper, _address_to_peer_name
from twisted.internet import reactor
import random


CNT = 20
cnt = 0


class Participant:

    def __init__(self, name):
        self.name = name

    def value_changed(self, peer, key, value):
        if key == '__heartbeat__':
            return
        #if peer != self.name:
        #    print self.name, "saw", peer, "change", key, "to", value
        if key == 'x':
            global cnt
            cnt += 1
            if cnt == CNT:
                print reactor.seconds()
                #print "DONE"
        if key == '/leader-election/vote':
            try:
                vote = self.gossiper.get_local_value(
                            '/leader-election/vote')
                # check consensus:
                for peer in self.gossiper.live_peers():
                    v = self.gossiper.get_peer_value(
                            peer, '/leader-election/vote')
                    if v != vote:
                        #print "no consensus", peer, "voted on", v, "(i like", vote, ")"
                        return
            except KeyError:
                #print "key error in vote"
                return
            #print "got consensus on votes"
            vote = self.gossiper.set_local_state(
                '/leader-election/master', v)
        elif key == '/leader-election/master':
            try:
                vote = self.gossiper.get_local_value('/leader-election/master')
                # check consensus:
                for peer in self.gossiper.live_peers():
                    v = self.gossiper.get_peer_value(peer, '/leader-election/master')
                    if v != vote:
                        return
            except KeyError:
                return
            print self.name, "WE GOT A NEW MASTER", vote, reactor.seconds()

    def peer_alive(self, peer):
        print self.name, "thinks", peer, "is alive", self.gossiper.get_peer_keys(peer)
        print self.gossiper.get_peer_value(peer, '/leader-election/priority')
        self._start_election()

    _election_timeout = None

    def _vote(self):
        self._election_timeout = None
        suggested_peer = self.gossiper.name
        arrogance = self.gossiper.get_local_value(
                '/leader-election/priority')
        for peer in self.gossiper.live_peers():
            p = self.gossiper.get_peer_value(
                    peer, '/leader-election/priority')
            if p > arrogance:
                suggested_peer = peer
                arrogance = p
        print self.name, "votes for", suggested_peer
        try:
            current_master = self.gossiper.get_local_value(
                '/leader-election/vote')
            if current_master == suggested_peer:
                #print self.name, "no need to update master"
                return
        except KeyError:
            pass
        self.gossiper.set_local_state('/leader-election/vote', suggested_peer)

    def _start_election(self):
        if self._election_timeout is not None:
            self._election_timeout.cancel()
        self._election_timeout = reactor.callLater(5, self._vote)

    def peer_dead(self, peer):
        print self.name, "thinks", peer, "is dead"
        self._start_election()

    def peer_stable(self, peer):
        print "stable", peer


members = []

for i in range(0, CNT):
    participant = Participant('127.0.0.1:%d' % (9000+i))
    gossiper = Gossiper(reactor, '127.0.0.1:%d' % (9000+i), participant)
    gossiper.set_local_state('/leader-election/priority', i)
    p = reactor.listenUDP(9000+i, gossiper)
    members.append((gossiper, p, participant))

for i in range(1, CNT):
    members[i][0].handle_new_peers(['127.0.0.1:9000'])

seed = members[0][0]

def prop_test():
    print "START PROP TEST"
    print reactor.seconds()
    seed.set_local_state('x', 'value')

pending = []

def kill_some():
    if len(members) > (CNT / 2):
        #i = random.randint(0, len(members) - 1)
        i = len(members) - 1
        gossiper, p, participant = members.pop(i)
        print "killing", p.getHost()
        p.stopListening()
    #reactor.callLater(5, kill_some)

def test():
    seed.set_local_state('test', 'value')
    reactor.callLater(5, prop_test)
    reactor.callLater(30, kill_some)

reactor.callWhenRunning(test)
reactor.run()
