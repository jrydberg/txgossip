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

import fnmatch


class LeaderElectionMixin:
    """Mixin for leader election among the nodes in the cluster.

    The C{value_changed} method should be called when any of the
    related keys are changed in the key-stores.

    The mixin will call C{leader_elected} when an election has taken
    place.

    When a node peforms an election, which it should do after any
    group change, it picks the node with the highest C{prio-key}
    value.  It updates its C{vote-key} with the name of that peer.
    When all peers have voted on the same peer, they one by one set
    their C{leader-key} to the same value as their C{vote-key}.  The
    election is over when all peers have reached a consensus on the
    C{leader-key} value.
    """

    PRIO_KEY = 'leader:priority'
    VOTE_KEY = 'leader:vote'
    LEADER_KEY = 'leader:leader'

    def __init__(self, clock):
        self.clock = clock
        self._election_timeout = None

    def _check_consensus(self, key):
        """Check if all peers have the same value for C{key}.

        Return the value if they all have the same value, otherwise
        return C{None}.
        """
        try:
            correct = self.gossiper.get_local_value(key)
            for peer in self.gossiper.live_peers():
                value = self.gossiper.get_peer_value(peer, key)
                if value != correct:
                    return None
        except KeyError:
            # One peer did not even have the key.
            return None
        else:
            return correct

    def value_changed(self, peer, key, value):
        """Inform about a change of a key-value pair.

        @param peer: The peer that changed a value.
        @param key: The key.
        @param value: The new value.

        @return: C{True} if this method acted on the change, or if it
           was unrelated.
        """
        if key == self.VOTE_KEY:
            leader = self._check_consensus(self.VOTE_KEY)
            if leader:
                self.gossiper.set_local_state(
                    self.LEADER_KEY, leader)
        elif key == self.LEADER_KEY:
            leader = self._check_consensus(self.LEADER_KEY)
            if leader:
                self.leader_elected(
                    self.gossiper.name == leader, leader)
        elif key == self.PRIO_KEY:
            self.start_election()

        return key in (self.VOTE_KEY, self.LEADER_KEY, self.PRIO_KEY)

    def _vote(self):
        """Perform an election."""
        self._election_timeout = None

        # Check if we're one of the peers that would like to become
        # master.
        vote, currp = None, None
        try:
            curr = self.gossiper.get_local_value(self.PRIO_KEY)
            if curr is not None:
                vote = self.gossiper.name
        except KeyError:
            pass

        for peer in self.gossiper.live_peers():
            try:
                prio = self.gossiper.get_peer_value(
                    peer, self.PRIO_KEY)
            except KeyError:
                continue
            if prio is None:
                # This peer did not want to become leader.
                continue
            elif curr is None:
                curr = prio
                vote = peer
            elif prio > curr:
                curr = prio
                vote = peer
            elif prio == curr:
                # We need to break the tie.
                if hash(peer) > hash(vote):
                    vote = peer

        # See if there's a need to change our vote, or if we'll stand
        # by our last vote.
        if self.VOTE_KEY in self.gossiper.keys():
            if self.gossiper.get_local_value(self.VOTE_KEY) == vote:
                return
        self.gossiper.set_local_state(self.VOTE_KEY, vote)

    def start_election(self):
        """Start an election.

        Elections should be started when the cluster membership view
        changes (i.e.g, when a peer joins the cluster, or when a peer
        dies).

        It is safe to call this while an election is taking place.
        """
        if self._election_timeout is not None:
            self._election_timeout.cancel()
        self._election_timeout = self.clock.callLater(5, self._vote)

    def leader_elected(self, is_leader, leader):
        """Notifcation about leader election result.

        @param is_leader: C{True} if this peer is the leader.
        @param leader: The address of the peer that is the leader.
        """


class KeyStoreMixin:
    """."""

    def __init__(self, clock, storage, ignore_keys=[]):
        """

        @param clock: Something that can report the time, normally a
            Twisted reactor.
        @param storage: A backing storage object that responds to the
            normal C{dict}-like protocol.
        @param ignore_keys: A sequence of keys that should not be
            replicated between the peers.
        """
        self.clock = clock
        self._storage = storage
        self._ignore_keys = ignore_keys

    def value_changed(self, peer, key, timestamp_value):
        """A peer has changed its value."""
        if key == '__heartbeat__':
            # We do not care about updates to the heartbeat value.
            return
        timestamp, value = timestamp_value
        if key in self._storage:
            current_timestamp, current_value = self._storage[key]
            if timestamp <= current_timestamp:
                return
        self._storage[key] = (timestamp, value)

    def __setitem__(self, key, value):
        self.gossiper.set_local_state(key,
            (self.clock.seconds(), value))

    def __getitem__(self, key):
        return self._storage[key][1]

    def get(self, key, default=None):
        if key in self._storage:
            return self[key]
        return default

    def keys(self, pattern=None):
        """Return a iterable of all available keys."""
        if pattern is None:
            return self.gossiper.keys()
        else:
            keys = self.gossiper.keys()
            return [key for key in keys
                    if fnmatch.fnmatch(key, pattern)]

    def __contains__(self, key):
        return key in self.keys()

    def timestamp_for_key(self, key):
        return self._storage[key][0]

    def synchronize_keys_with_peer(self, peer):
        """Synchronize keys with C{peer}.

        Will iterate through all the keys that C{peer} has and see if
        there's some values that are newer than ours.
        """
        for key in self.gossiper.get_peer_keys(peer):
            if key in self._ignore_keys:
                continue
            self.value_changed(peer, key, self.gossiper.get_peer_value(
                    peer, key))
