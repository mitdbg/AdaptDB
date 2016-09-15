# AdaptDB

AdaptDB is an adaptive storage manager for analytical database workloads in a distributed setting. 

It works by partitioning datasets across a cluster and incrementally refining data partitioning as queries are run. 

AdaptDB introduces a novel hyper join that avoids expensive data shuffling by identifying storage blocks of the joining tables that overlap on the join attribute, and only joining those blocks. Hyper join performs well when each block in one table overlaps with few blocks in the other table, since that will minimize the number of blocks that have to be accessed.

To minimize the number of overlapping blocks for common join queries, AdaptDB users smooth repartitioning to repartition small portions of the tables on join attributes as queries run. 

A prototype of AdaptDB running on top of Spark improves query performance by 2-3x on TPC-H as well as real-world dataset, versus a system that employs scans and shuffle-joins.