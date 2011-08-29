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

from txgossip.state import PeerState


class Scuttle(object):

    def __init__(self, peers, local_peer):
        self.peers = peers
        self.local_peer = local_peer

    def digest(self):
        digest = {}
        for peer, state in self.peers.items():
            digest[peer] = state.max_version_seen
        return digest

    def scuttle(self, digest):
        deltas_with_peer = []
        requests = {}
        new_peers = []
        for peer, digest_version in digest.items():
            if not peer in self.peers:
                requests[peer] = 0
                new_peers.append(peer)
            else:
                state = self.peers[peer]
                if state.max_version_seen > digest_version:
                    deltas_with_peer.append((
                            peer,
                            self.peers[peer].deltas_after_version(digest_version)
                            ))
                elif state.max_version_seen < digest_version:
                    requests[peer] = state.max_version_seen

        # Sort by peers with most deltas
        def sort_metric(a, b):
            return len(b[1]) - len(a[1])
        deltas_with_peer.sort(cmp=sort_metric)

        deltas = []
        for (peer, peer_deltas) in deltas_with_peer:
            for (key, value, version) in peer_deltas:
                deltas.append((peer, key, value, version))

        return deltas, requests, new_peers

    def update_known_state(self, deltas):
        for peer, key, value, version in deltas:
            self.peers[peer].update_with_delta(
                str(key), value, version)

    def fetch_deltas(self, requests):
        deltas = []
        for peer, version in requests.items():
            peer_deltas = self.peers[peer].deltas_after_version(
                version)
            for (key, value, version) in peer_deltas:
                deltas.append((peer, key, value, version))
        return deltas
