# txgossip #

This is a port of node-gossip (also on github) to python.

We will make some changes, but it will probably stay true
to the architecture:

  http://wiki.apache.org/cassandra/ArchitectureGossip


# Functionality #

What `txgossip` provides is a cluster communication mechanism.
Participants in the cluster get notification about joining
and failing peers.  Each peer also provides a key-value store
of data that the other peers can access.  Using this data store
functionality like leader election can be implemented.

