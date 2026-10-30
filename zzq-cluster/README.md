# zzq-cluster

Viewstamped Replication for a three-node zzq cluster: one group, one
leader, ops held in RAM on the leader and a follower before they commit.
Engine crate of `zzq-broker`, not semver.

- `node::Node`: one node, sans-I/O. Normal operation with a leader lease
  its followers back, elections, images of the application's state for a
  node that has none, recovery after a crash, exit dumps for a restart.
- Below it, the replication cores copied from Ozzy's `ozzy-replication`,
  with their wire codecs.
- `tests/cluster.rs`: three nodes on a simulated network from one seed,
  with drops, delays, splits, crashes, clean stops and restarts.
  `ZZQ_SIM_SEEDS=<n>` runs more seeds.
