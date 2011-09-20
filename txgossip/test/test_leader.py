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

from mockito import mock, when, verify, verifyNoMoreInteractions

from twisted.trial import unittest
from twisted.internet import task

from txgossip.recipies import LeaderElectionMixin


class KeyStoreTestCase(unittest.TestCase):
    """Test cases for the key-value store mixin."""

    def setUp(self):
        self.clock = task.Clock()
        self.storage = {}
        self.election = LeaderElectionMixin(self.clock)
        self.gossiper = mock()
        self.gossiper.name = 'self'
        self.election.make_connection(self.gossiper)

    def test_make_connection_starts_election(self):
        self.assertTrue(self.election._election_timeout)

    def test_waits_for_consensus_on_vote_before_leader_elected(self):
        peer = mock()
        when(peer).get(self.election.VOTE_KEY).thenReturn('a')
        self.gossiper.live_peers = [peer]
        when(peer).keys().thenReturn([self.election.VOTE_KEY])
        when(self.gossiper).get(self.election.VOTE_KEY).thenReturn('b')
        self.election.value_changed('peer-a', self.election.VOTE_KEY,
                                    'a')
        verify(self.gossiper).get(self.election.VOTE_KEY)
        verify(peer).keys()
        verify(peer).get(self.election.VOTE_KEY)
        verifyNoMoreInteractions(self.gossiper)
        verifyNoMoreInteractions(peer)

    def test_leader_elected_when_consensus_reached_on_votes(self):
        peer = mock()
        when(peer).get(self.election.VOTE_KEY).thenReturn('a')
        when(peer).keys().thenReturn([self.election.VOTE_KEY])
        self.gossiper.live_peers = [peer]
        when(self.gossiper).get(self.election.VOTE_KEY).thenReturn('a')
        self.election.value_changed('peer-a', self.election.VOTE_KEY, 'a')
        verify(self.gossiper).get(self.election.VOTE_KEY)
        verify(self.gossiper).set(self.election.LEADER_KEY, 'a')
        verify(peer).keys()
        verify(peer).get(self.election.VOTE_KEY)
        verifyNoMoreInteractions(self.gossiper)
        verifyNoMoreInteractions(peer)

    def test_waits_for_consensus_on_leader_before_proclaiming(self):
        peer = mock()
        when(peer).get(self.election.LEADER_KEY).thenReturn('a')
        when(peer).keys().thenReturn([self.election.LEADER_KEY])
        self.gossiper.live_peers = [peer]
        when(self.gossiper).get(self.election.LEADER_KEY).thenReturn('b')
        self.election.value_changed('peer', self.election.LEADER_KEY, 'a')
        verify(self.gossiper).get(self.election.LEADER_KEY)
        verify(peer).keys()
        verify(peer).get(self.election.LEADER_KEY)
        verifyNoMoreInteractions(self.gossiper)
        verifyNoMoreInteractions(peer)

    def test_leader_proclaimed_when_consensus_reached_on_leader(self):
        peer = mock()
        when(peer).keys().thenReturn([self.election.LEADER_KEY])
        when(peer).get(self.election.LEADER_KEY).thenReturn('a')
        self.gossiper.live_peers = [peer]
        when(self.gossiper).get(self.election.LEADER_KEY).thenReturn('a')
        self.election.value_changed('peer', self.election.LEADER_KEY, 'a')
        self.assertEquals(self.election.is_leader, False)

    def test_sets_is_leader_when_leader_is_gossiper(self):
        self.gossiper.live_peers = []
        when(self.gossiper).get(self.election.LEADER_KEY).thenReturn('self')
        self.election.value_changed('self', self.election.LEADER_KEY, 'self')
        self.assertEquals(self.election.is_leader, True)

    def test_vote_for_peer_with_highest_priority(self):
        peer = mock()
        peer.name = 'peer'
        when(peer).get(self.election.PRIO_KEY).thenReturn(1)
        when(self.gossiper).get(self.election.PRIO_KEY).thenReturn(0)
        self.gossiper.live_peers = [peer]
        self.election._vote()
        verify(self.gossiper).set(self.election.VOTE_KEY, 'peer')

    def test_breaks_tie_using_hash_function(self):
        peer = mock()
        peer.name = 'peer'
        when(peer).get(self.election.PRIO_KEY).thenReturn(0)
        when(self.gossiper).get(self.election.PRIO_KEY).thenReturn(0)
        self.gossiper.live_peers = [peer]
        self.election._vote()
        verify(self.gossiper).set(self.election.VOTE_KEY, 'peer')
        self.assertTrue(hash(peer.name) > hash(self.gossiper.name))

    def test_ingores_peer_that_has_no_priority(self):
        peer = mock()
        peer.name = 'peer'
        when(peer).get(self.election.PRIO_KEY).thenReturn(None)
        when(self.gossiper).get(self.election.PRIO_KEY).thenReturn(0)
        self.gossiper.live_peers = [peer]
        self.election._vote()
        verify(self.gossiper).set(self.election.VOTE_KEY, 'self')

    def test_ingores_self_if_no_priority_set(self):
        peer = mock()
        peer.name = 'peer'
        when(peer).get(self.election.PRIO_KEY).thenReturn(0)
        when(self.gossiper).get(self.election.PRIO_KEY).thenReturn(None)
        self.gossiper.live_peers = [peer]
        self.election._vote()
        verify(self.gossiper).set(self.election.VOTE_KEY, 'peer')


