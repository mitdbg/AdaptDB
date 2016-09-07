TPCH3

(lineitem ⋈ orders) ⋈ customer

l_orderkey = o_orderkey, o_custkey_c_custkey

TPCH5

((customer ⋈ orders) ⋈ (lineitem ⋈ supplier))

o_custkey = c_custkey, o_orderkey = l_orderkey, l_suppkey = s_suppkey

TPCH6

lineitem

TPCH8

(lineitem ⋈ part) ⋈  (orders ⋈ customer)

l_partkey = p_partkey, l_orderkey = o_orderkey, o_custkey = c_custkey

TPCH10

(lineitem ⋈ orders) ⋈ customer

l_orderkey = o_orderkey, o_custkey = c_custkey

TPCH12

lineitem ⋈ orders

l_orderkey = o_orderkey

TPCH14

lineitem ⋈ part

l_partkey = p_parkey

TPCH19

lineitem ⋈ part

l_partkey = p_parkey

