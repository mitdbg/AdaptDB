#fab setup:join_lineitem create_table_info
#fab setup:join_lineitem bulk_sample_gen
#fab setup:join_lineitem create_robust_tree
#fab setup:join_lineitem write_partitions


#fab setup:join_orders create_table_info
#fab setup:join_orders bulk_sample_gen
#fab setup:join_orders create_robust_tree
#fab setup:join_orders write_partitions


#fab setup:join_part create_table_info
#fab setup:join_part bulk_sample_gen
#fab setup:join_part create_robust_tree
#fab setup:join_part write_partitions


fab setup:join_supplier create_table_info
fab setup:join_supplier bulk_sample_gen
fab setup:join_supplier create_robust_tree
fab setup:join_supplier write_partitions


#fab setup:join_customer create_table_info
#fab setup:join_customer bulk_sample_gen
#fab setup:join_customer create_robust_tree
#fab setup:join_customer write_partitions
