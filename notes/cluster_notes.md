### How to choose shards, replicas and cluster size: Rules of Thumb.

sh      = shards
rf      = replication factor. replicas = 0 implies rf = 1, or 1 replica of each shard.

pm      = running data_esnode processes per machine
N       = number of machines

n_cores = number of cpu cores per machine
n_disks = number of disks per machine

* You must have at least as many data_esnodes as 
  Mandatory:  (sh * rf) < (pm * N)

  Shards:     shard size < 10GB

More shards = more parallel writes