fab setup:local_lineitem bulk_sample_gen
fab setup:local_lineitem create_robust_tree
fab setup:local_lineitem write_partitions

fab setup:local_orders bulk_sample_gen
fab setup:local_orders create_robust_tree
fab setup:local_orders write_partitions

fab setup:local_part bulk_sample_gen
fab setup:local_part create_robust_tree
fab setup:local_part write_partitions

fab setup:local_supplier bulk_sample_gen
fab setup:local_supplier create_robust_tree
fab setup:local_supplier write_partitions

fab setup:local_customer bulk_sample_gen
fab setup:local_customer create_robust_tree
fab setup:local_customer write_partitions
