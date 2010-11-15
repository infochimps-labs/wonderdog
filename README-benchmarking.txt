# . jack up batch size and see effect on rec/sec, find optimal
# . run multiple mappers with one data es_node with optimal batch size, refind if necessary
# . work data es_node heavily but dont drive it into the ground
# . tune lucene + jvm options for data es_node

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
