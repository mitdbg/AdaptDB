fab setup:orders1000 create_table_info
fab setup:orders1000 bulk_sample_gen
fab setup:orders1000 create_join_robust_tree
fab setup:orders1000 write_join_partitions


fab setup:part1000 create_table_info
fab setup:part1000 bulk_sample_gen
fab setup:part1000 create_join_robust_tree
fab setup:part1000 write_join_partitions


fab setup:supplier1000 create_table_info
fab setup:supplier1000 bulk_sample_gen
fab setup:supplier1000 create_join_robust_tree
fab setup:supplier1000 write_join_partitions


fab setup:customer1000 create_table_info
fab setup:customer1000 bulk_sample_gen
fab setup:customer1000 create_join_robust_tree
fab setup:customer1000 write_join_partitions

fab setup:lineitem1000 create_table_info
fab setup:lineitem1000 bulk_sample_gen
fab setup:lineitem1000 create_join_robust_tree
fab setup:lineitem1000 write_join_partitions


fab setup:join_mh create_table_info
fab setup:join_mh bulk_sample_gen
fab setup:join_mh create_join_robust_tree
fab setup:join_mh write_join_partitions

fab setup:join_mhl create_table_info
fab setup:join_mhl bulk_sample_gen
fab setup:join_mhl create_join_robust_tree
fab setup:join_mhl write_join_partitions

fab setup:join_sf create_table_info
fab setup:join_sf bulk_sample_gen
fab setup:join_sf create_join_robust_tree
fab setup:join_sf write_join_partitions

