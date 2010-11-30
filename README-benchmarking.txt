1 c1.xl 	4es/12sh     	768m buffer     1400m heap
 2,584,346,624	     255,304	0h14m25	    865	    295	   2917

1 m1.xl 	4es/12sh       	1500m buffer    3200m heap 
    79,364,096	     464,701	0h01m02	     62	   7495	   1250
   210,305,024	   1,250,000	0h02m39	    159	   7861	   1291
   429,467,863	   2,521,538	0h03m28	    208	  12122	   2016
   
1 m1.xl 	4es/12sh  	4hdp	1800m buffer    3200m heap      300000 tlog
   429,467,863	   2,521,538	0h03m11	    191	  13201	   2195

1 m1.xl		4es/12sh	4hdp	1800m buffer	2400m heap	100000 tlog	1000 batch	lzw compr	ulimit-l-unlimited (and in all following)
                                0h03m47
                                
1 m1.xl		4es/12sh	4hdp	1800m buffer	2400m heap	200000 tlog	1000 batch	no compr	
                                0h3m22
  again on top of data already loaded                                
                                0h3m16                                

1 m1.xl		4es/12sh	64hdp	1800m buffer	2800m heap	200000 tlog	50000 batch	no compr	
   433,782,784	   2,250,000	0h01m17  (froze up on mass assault once 50k batch was reached)
  
1 m1.xl		4es/12sh	64hdp	1800m buffer	2800m heap	200000 tlog	5000 batch	no compr	
   785,514,496	   4,075,000	0h05m59	    359	  11350	   2136         cpu 4x70%
 1,207,500,800	   6,270,000	0h08m26	    506	  12391	   2330

1 m1.xl		4es/12sh	64hdp	1800m buffer	2800m heap	200000 tlog	5000 batch	no compr	
   163,512,320	     845,000	0h01m49	    109	   7752	   1464         cpu 4x75% ios 6k-8k x4 if 2800/440 ram 13257/15360MB
   641,990,656	   3,345,000	0h04m41	    281	  11903	   2231
   896,522,559	   4,683,016	0h06m11	    371	  12622	   2359
 1,131,916,976	   5,937,895	0h07m05	    425	  13971	   2600

1 m1.xl		4es/12sh	16hdp	1800m buffer	2800m heap	200000 tlog	5000 batch	no compr	
    74,383,360	     385,000	0h01m50	    110	   3500	    660
   286,720,000	   1,495,000	0h02m21	    141	  10602	   1985
   461,701,120	   2,410,000	0h03m30	    210	  11476	   2147
   733,413,376	   3,830,000	0h05m10	    310	  12354	   2310
 1,131,916,976	   5,937,895	0h07m16	    436	  13619	   2535

1 m1.xl		4es/12sh	64hdp	1800m buffer	2800m heap	200000 tlog	1000 batch	no compr	
   156,958,720	     813,056	0h01m35	     95	   8558	   1613
   305,135,616	   1,586,176	0h02m25	    145	  10939	   2055
   446,300,160	   2,323,456	0h03m10	    190	  12228	   2293
   690,028,544	   3,594,240	0h04m40	    280	  12836	   2406
   927,807,418	   4,850,093	0h06m10	    370	  13108	   2448
 1,131,916,976	   5,937,895	0h06m55	    415	  14308	   2663

1 m1.xl		4es/12sh	16hdp	1800m buffer	2800m heap	200000 tlog	1024 batch	no compr	
   234,749,952	   1,222,656	0h02m08	    128	   9552	   1791
   713,097,216	   3,723,264	0h04m56	    296	  12578	   2352
 1,131,916,976	   5,937,895	0h06m49	    409	  14518	   2702

1 m1.xl		4es/12sh	20hdp	1800m buffer	2800m heap	200000 tlog	1024 batch	no compr	mergefac 40
   190,971,904	     994,304	0h01m55	    115	   8646	   1621
   326,107,136	   1,699,840	0h02m52	    172	   9882	   1851
   707,152,365	   3,709,734	0h04m51	    291	  12748	   2373  672 files
 again:
   187,170,816	     973,824	0h01m49	    109	   8934	   1676
   707,152,365	   3,709,734	0h05m39	    339	  10943	   2037   1440 files ; 18 *.tis typically 4.3M
 again:
   707,152,365	   3,709,734	0h04m54	    294	  12618	   2348   2052 files ; 28 *.tis typically 4.3M

1 m1.xl		4es/12sh	20hdp	1800m buffer	2800m heap	50_000 tlog	1024 batch	no compr	mergefac 20 (and in following)
   349,372,416	   1,821,696	0h02m42	    162	  11245	   2106
   707,152,365	   3,709,734	0h04m43	    283	  13108	   2440

1 m1.xl		4es/4sh 	20hdp	1800m buffer	2800m heap	200_000 tlog	1024 batch	no compr	64m engine.ram_buffer_size -- 3s ping_interval -- oops 10s refresh
   253,689,856	   1,321,984	0h02m48	    168	   7868	   1474
   707,152,365	   3,709,734	0h05m55	    355	  10449	   1945

1 m1.xl		4es/4sh 	20hdp	1800m buffer	2800m heap	200_000 tlog	1024 batch	no compr	256m engine.ram_buffer_size -- 3s ping_interval
   707,152,365	   3,709,734	0h04m31	    271	  13689	   2548
   
1 m1.xl		4es/4sh 	20hdp	1800m buffer	2800m heap	200_000 tlog	1024 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval
   707,152,365	   3,709,734	0h04m08	    248	  14958	   2784

1 m1.xl		4es/4sh 	20hdp	1800m buffer	2800m heap	200_000 tlog	1024 batch	no compr	768m engine.ram_buffer_size -- 3s ping_interval
   707,152,365	   3,709,734	0h04m47	    287	  12925	   2406
  again
   707,152,365	   3,709,734	0h04m27	    267	  13894	   2586

1 m1.xl		4es/4sh 	20hdp	 768m buffer	2800m heap	200_000 tlog	1024 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval
   707,152,365	   3,709,734	0h04m14	    254	  14605	   2718

1 c1.xl		4es/4sh 	20hdp	 768m buffer	1200m heap	200_000 tlog	1024 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval
   707,152,365	   3,709,734	0h02m55	    175	  21198	   3946  ios 11282 ifstat 3696.26    695.26

1 c1.xl		4es/4sh 	40hdp	 768m buffer	1200m heap	200_000 tlog	4096 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval
   707,912,831	   3,713,598	0h03m05	    185	  20073	   3736

1 c1.xl		4es/4sh 	40hdp	 768m buffer	1200m heap	200_000 tlog	1024 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval
   707,912,831	   3,713,598	0h02m59	    179	  20746	   3862

1 c1.xl		4es/4sh 	20hdp	 256m buffer	1200m heap	200_000 tlog	1024 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval
   707,152,365	   3,709,734	0h02m53	    173	  21443	   3991

1 c1.xl		4es/4sh 	20hdp	 512m buffer	1200m heap	200_000 tlog	1024 batch	no compr	768m engine.ram_buffer_size -- 3s ping_interval
   707,152,365	   3,709,734	0h03m00	    180	  20609	   3836


8 c1.xl		32es/32sh 	14hdp/56  512m buffer	1200m heap	200_000 tlog	1024 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval
 1,115,291,648	   5,814,272	0h01m44	    104	   6988	   1309	      8	  55906	  10472
 2,779,840,512	  14,540,800	0h06m34	    394	   4613	    861	      8	  36905	   6890
 6,100,156,416	  32,508,928	0h14m51	    891	   4560	    835	      8	  36485	   6685
 (killed)

8 c1.xl		24es/24sh 	14hdp/56  256m buffer	1200m heap	200_000 tlog	1024 batch	no compr	384m engine.ram_buffer_size -- 3s ping_interval
   980,221,952	   5,107,662	0h01m28	     88	   7255	   1359	      8	  58041	  10877
 1,815,609,344	   9,483,259	0h01m59	    119	   9961	   1862	      8	  79691	  14899
 4,451,270,656	  23,694,336	0h04m06	    246	  12039	   2208	      8	  96318	  17670
 6,713,269,627	  35,778,171	0h06m00	    360	  12422	   2276	      8	  99383	  18210

8 c1.xl		24es/24sh 	14hdp/140  512m buffer	1200m heap	200_000 tlog	1024 batch	no compr	384m engine.ram_buffer_size -- 3s ping_interval
 4,743,036,929	  24,825,856	0h04m39	    279	  11122	   2075	      8	  88981	  16601
 8,119,975,937	  42,889,216	0h07m00	    420	  12764	   2360	      8	 102117	  18880
17,273,994,924	  91,991,529	0h15m14	    914	  12580	   2307	      8	 100647	  18456
23,598,696,768	 123,812,641	0h24m04	   1444	  10717	   1994	      8	  85742	  15959


8 m1.xl		32es/32sh 	14hdp/53  1800m buffer	2800m heap	200_000 tlog	1024 batch	no compr	512m engine.ram_buffer_size -- 3s ping_interval -- merge_factor30
   306,296,262	   1,608,526	0h01m18	     78	   2577	    479	      8	  20622	   3834
 1,814,083,014	   9,564,301	0h02m33	    153	   7813	   1447	      8	  62511	  11578
 2,837,886,406	  15,030,140	0h04m49	    289	   6500	   1198	      8	  52007	   9589
 3,928,208,838	  21,039,950	0h06m22	    382	   6884	   1255	      8	  55078	  10042
 6,322,378,160	  33,875,546	0h11m28	    688	   6154	   1121	      8	  49237	   8974

8 c1.xl		24es/24sh 	14hdp/140  512m buffer	1200m heap	200_000 tlog	4096 batch	no compr	256m engine.ram_buffer_size -- 3s ping_interval -- merge_factor 30 
 4,717,346,816	  24,855,996	0h04m55	    295	  10532	   1952	      8	  84257	  15616
 9,735,831,552	  51,896,969	0h09m23	    563	  11522	   2110	      8	  92179	  16887


(200910) 
 2,746,875,904	  10,555,392	0h02m50	    170	   7761	   1972	      8	  62090	  15779
43,201,339,007	 166,049,864	0h35m06	   2106	   9855	   2504	      8	  78846	  20032


 

slug=tweet-2009q3pre ; curl -XGET 'http://10.99.10.113:9200/_flush/' ; curl -XPUT "http://10.99.10.113:9200/$slug/" ; rake -f ~/ics/backend/wonderdog/java/Rakefile ; ~/ics/backend/wonderdog/java/bin/wonderdog --rm --index_name=$slug --bulk_size=4096 --object_type=tweet /tmp/tweet_by_month-tumbled/"tweet-200[678]" /tmp/es_bulkload_log/$slug


sudo kill `ps aux | egrep '^61021' | cut -c 10-15`

for node in '' 2 3 ; do echo $node ; sudo node=$node ES_MAX_MEM=1600m ~/ics/backend/wonderdog/config/run_elasticsearch-2.sh  ; done


for node in '' 2 3 4 ; do echo $node ; sudo node=$node ES_MAX_MEM=1200m  ~/ics/backend/wonderdog/config/run_elasticsearch-2.sh  ; done
sudo kill `ps aux | egrep '^61021' | cut -c 10-15` ; sleep 10 ; sudo rm -rf /mnt*/elasticsearch/* ; ps auxf | egrep '^61021' ; zero_log /var/log/elasticsearch/hoolock.log

ec2-184-73-41-228.compute-1.amazonaws.com   
   
Query for success:
  curl -XGET 'http://10.195.10.207:9200/tweet/tweet/_search?q=text:mrflip' | ruby -rubygems -e 'require "json" ; puts JSON.pretty_generate(JSON.load($stdin))' 

Detect settings:
  grep ' with ' /var/log/elasticsearch/hoolock.log  | egrep 'DEBUG|INFO' | cut -d\] -f2,3,5- | sort | cutc | uniq -c

Example index sizes:  
  ls -lRhart /mnt*/elasticsearch/data/hoolock/nodes/*/indices/tweet/0/*/*.{tis,fdt}




def dr(line) ; sbytes,srecs,time,mach,*_ = line.strip.split(/\s+/) ; bytes = sbytes.gsub(/\D/,"").to_i ; recs = srecs.gsub(/\D/,"").to_i ; mach=mach.to_i ; mach = 1 if mach == 0 ; s,m,h = [0,0,0,time.split(/\D/)].flatten.reverse.map(&:to_i) ; tm = (3600*h + 60*m + s) ; results = "%14s\t%12s\t%01dh%02dm%02d\t%7d\t%7d\t%7d\t%7d\t%7d\t%7d"%[sbytes, srecs, h,m,s, tm, recs/tm/mach, bytes/tm/1024/mach, mach, recs/tm, bytes/tm/1024,  ] ; puts results ; results ; end







 

   



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
