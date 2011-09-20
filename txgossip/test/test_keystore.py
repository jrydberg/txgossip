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

from mockito import mock, when, verify

from twisted.trial import unittest
from twisted.internet import task

from txgossip.recipies import KeyStoreMixin


class KeyStoreTestCase(unittest.TestCase):
    """Test cases for the key-value store mixin."""

    def setUp(self):
        self.clock = task.Clock()
        self.storage = {}
        self.keystore = KeyStoreMixin(self.clock, self.storage)
        self.gossiper = mock()
        self.gossiper.name = 'self'
        self.keystore.make_connection(self.gossiper)

    def test_replicate_when_remote_peer_changed_value(self):
        self.keystore.value_changed('a', 'k', (0, 'value'))
        verify(self.gossiper).set('k', (0, 'value'))
        self.assertNotIn('k', self.storage)

    def test_ignore_replication_when_remote_peer_has_old_value(self):
        self.storage['k'] = (1, 'value')
        self.keystore.value_changed('a', 'k', (0, 'value'))
        verify(self.gossiper, times=0).set('k', (0, 'value'))

    def test_persist_value_when_set_on_local_peer(self):
        self.keystore.value_changed('self', 'k', (0, 'value'))
        self.assertIn('k', self.storage)
        self.assertEquals(self.storage['k'], (0, 'value'))

    def test_keys_returns_all_keys_in_gossiper(self):
        when(self.gossiper).keys().thenReturn(['a', 'b'])
        self.assertEquals(self.keystore.keys(), ['a', 'b'])

    def test_keys_can_be_filtered(self):
        when(self.gossiper).keys().thenReturn(['ab', 'ba'])
        self.assertEquals(self.keystore.keys('b*'), ['ba'])

    def test_contains_use_keys_from_gossiper(self):
        when(self.gossiper).keys().thenReturn(['a', 'b'])
        self.assertIn('a', self.keystore)

    def test_get_results_default_value_if_value_not_present(self):
        when(self.gossiper).keys().thenReturn([])
        self.assertEquals(self.keystore.get('a', '!'), '!')

    def test_get_returns_value_from_gossip_state(self):
        when(self.gossiper).keys().thenReturn(['a'])
        when(self.gossiper).get('a').thenReturn((0, '!'))
        self.assertEquals(self.keystore.get('a'), '!')
