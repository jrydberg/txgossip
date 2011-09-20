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

    def __init__(self, clock, vote_delay=5):
        self.clock = clock
        self._election_timeout = None
        self._gossiper = None
        self.is_leader = None
        self.vote_delay = vote_delay

    def make_connection(self, gossiper):
        self._gossiper = gossiper
        self.start_election()

    def _check_consensus(self, key):
        """Check if all peers have the same value for C{key}.

        Return the value if they all have the same value, otherwise
        return C{None}.
        """
        correct = self._gossiper.get(key)
        for peer in self._gossiper.live_peers:
            if not key in peer.keys():
                return None
            value = peer.get(key)
            if value != correct:
                return None
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
                self._gossiper.set(self.LEADER_KEY, leader)
        elif key == self.LEADER_KEY:
            leader = self._check_consensus(self.LEADER_KEY)
            if leader:
                self.leader_elected(
                    self._gossiper.name == leader, leader)
        elif key == self.PRIO_KEY:
            self.start_election()

        return key in (self.VOTE_KEY, self.LEADER_KEY, self.PRIO_KEY)

    def _vote(self):
        """Perform an election."""
        self._election_timeout = None

        # Check if we're one of the peers that would like to become
        # master.
        vote = None
        curr = self._gossiper.get(self.PRIO_KEY)
        if curr is not None:
            vote = self._gossiper

        for peer in self._gossiper.live_peers:
            prio = peer.get(self.PRIO_KEY)
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
                if hash(peer.name) > hash(vote.name):
                    vote = peer
        self._gossiper.set(self.VOTE_KEY, vote.name)

    def start_election(self):
        """Start an election.

        Elections should be started when the cluster membership view
        changes (i.e.g, when a peer joins the cluster, or when a peer
        dies).

        It is safe to call this while an election is taking place.
        """
        if self._election_timeout is not None:
            self._election_timeout.cancel()
        self._election_timeout = self.clock.callLater(self.vote_delay,
                self._vote)

    def leader_elected(self, is_leader, leader):
        """Notifcation about leader election result.

        @param is_leader: C{True} if this peer is the leader.
        @param leader: The address of the peer that is the leader.
        """
        self.is_leader = is_leader

    def peer_dead(self, peer):
        """A peer is dead."""
        self.start_election()

    def peer_alive(self, peer):
        """A peer is alive."""
        self.start_election()


class KeyStoreMixin:
    """Mixin that implements a distributed key-value store."""

    def __init__(self, clock, storage, ignore_keys=[]):
        """Initialize key-value store mixin.

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
        self._gossiper = None

    def make_connection(self, gossiper):
        self._gossiper = gossiper

    def persist_key_value(self, key, timestamped_value):
        self._storage[key] = timestamped_value
        if hasattr(self._storage, 'sync'):
            self._storage.sync()

    def replicate_key_value(self, peer, key, timestamped_value):
        timestamp, value = timestamped_value
        if key in self._storage:
            current_timestamp, current_value = self._storage[key]
            if timestamp <= current_timestamp:
                return
        # We replicate the value.
        self._gossiper.set(key, timestamped_value)

    def value_changed(self, peer, key, timestamp_value):
        """A peer has changed its value."""
        if key == '__heartbeat__' or key in self._ignore_keys:
            return
        if peer.name == self._gossiper.name:
            self.persist_key_value(key, timestamp_value)
        else:
            self.replicate_key_value(peer, key, timestamp_value)

    def set(self, key, value):
        self._gossiper.set(key, [self.clock.seconds(), value])

    def __setitem__(self, key, value):
        self.set(key, value)

    def __getitem__(self, key):
        return self._gossiper.get(key)[1]

    def get(self, key, default=None):
        if key in self.keys():
            return self[key]
        return default

    def keys(self, pattern=None):
        """Return a iterable of all available keys."""
        if pattern is None:
            return self._gossiper.keys()
        else:
            keys = self._gossiper.keys()
            return [key for key in keys
                    if fnmatch.fnmatch(key, pattern)]

    def load_from(self, storage):
        for key in storage:
            if key not in self._ignore_keys:
                self._gossiper.set(key, storage[key])

    def __contains__(self, key):
        return key in self.keys()

    def peer_dead(self, peer):
        """A peer is dead."""

    def peer_alive(self, peer):
        """A peer is alive."""
