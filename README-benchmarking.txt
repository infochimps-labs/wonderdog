# . jack up batch size and see effect on rec/sec, find optimal
# . run multiple mappers with one data es_node with optimal batch size, refind if necessary
# . work data es_node heavily but dont drive it into the ground
# . tune lucene + jvm options for data es_node

14 files, 3 hadoop nodes w/ 3 tasktrackers each  27 min
14 files, 3 hadoop nodes w/ 5 tasktrackers each 22 min

12 files @ 500k lines -- 3M rec --  3 hdp/2 tt -- 2 esnodes -- 17m


6 files  @ 100k = 600k rec -- 3hdp/2tt -- 1 es machine/2 esnodes -- 3m30
6 files  @ 100k = 600k rec -- 3hdp/2tt -- 1 es machine/4 esnodes -- 3m20



5 files, 3 nodes, 


Did 2,400,000 recs 24 tasks 585,243,042 bytes -- 15:37 on 12 maps/3nodes

Did _optimize
real	18m29.548s	user	0m0.000s	sys	0m0.000s	pct	0.00


java version "1.6.0_20"
Java(TM) SE Runtime Environment (build 1.6.0_20-b02)
Java HotSpot(TM) 64-Bit Server VM (build 16.3-b01, mixed mode)


===========================================================================


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
