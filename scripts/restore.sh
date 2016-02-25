#!/bin/sh

hadoop dfs -rmr /user/yilu/lineitem
hadoop dfs -rmr /user/yilu/orders
hadoop dfs -cp /user/yilu/lineitem-backup /user/yilu/lineitem
hadoop dfs -cp /user/yilu/orders-backup /user/yilu/orders
