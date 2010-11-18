# . jack up batch size and see effect on rec/sec, find optimal
# . run multiple mappers with one data es_node with optimal batch size, refind if necessary
# . work data es_node heavily but dont drive it into the ground
# . tune lucene + jvm options for data es_node

14 files, 3 hadoop nodes, 3 tasktrackers each 27 min
14 files, 3 hadoop nodes, 5 tasktrackers each 22 min


The refresh API allows to explicitly refresh an one or more index, making all
operations performed since the last refresh available for search. The (near)
real-time capabilities depends on the index engine used. For example, the robin
one requires refresh to be called, but by default a refresh is scheduled
periodically.

curl -XPOST 'http://localhost:9200/twitter/_refresh'

The refresh API can be applied to more than one index with a single call, or even on _all the indices.



runs:
  - es_machine: m1.xlarge
    es_nodes: 1
    es_max_mem: 1500m
    bulk_size: 5
    maps: 1
    records: 100000
    shards: 12
    replicas: 1
    merge_factor: 100
    thread_count: 32
    lucene_buffer_size: 256mb
    runtime: 108s
    throughput: 1000 rec/sec
  - es_machine: m1.xlarge
    es_nodes: 1
    bulk_size: 5
    maps: 1
    records: 100000
    shards: 12
    replicas: 1
    merge_factor: 1000
    thread_count: 32
    lucene_buffer_size: 256mb
    runtime: 77s
    throughput: 1300 rec/sec
  - es_machine: m1.xlarge
    es_nodes: 1
    bulk_size: 5
    maps: 1
    records: 100000
    shards: 12
    replicas: 1
    merge_factor: 10000
    thread_count: 32
    lucene_buffer_size: 512mb
    runtime: 180s
    throughput: 555 rec/sec
