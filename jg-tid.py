# !/bin/python
import psycopg2
import re
from time import time
import csv 
import sys

def main():

	# python filename
	# sigma = float(sys.argv[1])

	joinQueries = [ "select o_orderkey, l_orderkey from orders2_s0_5_z0 join lineitem_s0_5_z2 on o_orderkey = l_orderkey",
					"select p_partkey, l_partkey from part_s0_5_z2, lineitem_s0_5_z2 where p_partkey = l_partkey;",
					"select s_suppkey, l_suppkey from supplier_s1_z0_5, lineitem_s0_5_z2 where s_suppkey= l_suppkey;"]

		
	conn = psycopg2.connect(host="/tmp/", database="popowskm", user="popowskm", port="5450")
	print("connect successfully",conn)

	loop = 1 
	for joinQuery in joinQueries:
		for _ in range(loop):
			timeForKs = measureTimeForKs(conn, joinQuery)


# measures the running time of a join query for a given value of k 
def measureTimeForKs(conn, joinQuery):
	cur = conn.cursor()
	print("Progressive-Merge Join")
	cur.execute('set enable_material=off;')
	cur.execute('set max_parallel_workers_per_gather=0;')
	cur.execute('set enable_hashjoin=off;')
	cur.execute('set enable_mergejoin=on;')
	cur.execute('set enable_indexonlyscan=off;')
	cur.execute('set enable_indexscan=off;')
	cur.execute('set enable_block=off;')
	cur.execute('set enable_bitmapscan=off;')
	cur.execute('set enable_fastjoin=off;')
	cur.execute('set enable_seqscan=off;')
	cur.execute('set enable_fliporder=off;')
	
	cur = conn.cursor('cur_uniq')
	cur.itersize = 1

	start = time()
	print(joinQuery)
	print("start time",start)
	cur.execute(joinQuery)
	print('  time before fetch: %f sec' % (time() - start))
	fetched = 0
	start = time()
	# prev = start
	# factor = sigma
	# weightedTime = 0
	res = list()
	for fetch in cur:
		fetched += 1
		current = time()
		# weightedTime += (current-prev)*factor
		# prev = current
		# factor *= sigma
		if fetched in [20,50,100,500,1000,2000,30000,60000,120000,240000,500000,1000000]:
			joinTime = current - start
			print(fetched,joinTime)
			
	joinTime = time() - start
	print('  time %.2f sec' % (joinTime))
	res.append(joinTime * 1000)
	print(joinTime)
	cur.close()

	return res

# def prep(conn, z, s):
# 	prepLineitem(conn, z, s)
# 	# prepOrder(conn, z, s)

# # creates a shuffled copy of lineitem table 
# def prepLineitem(conn, z, s):
# 	print('Prep with z: ' + z + ' s: ' + s)
# 	tableName  = 'lineitem_s' + s + '_z' + z
# 	dropLineitemTable = 'drop table if exists tmp_lineitem;'
# 	createLineitemTable = 'create table tmp_lineitem as select * from ' + tableName  + ' order by random();'
# 	with conn.cursor() as cur:
# 		print('  updating lineitem table')
# 		cur.execute(dropLineitemTable)
# 		cur.execute(createLineitemTable)

# # creates a shuffled copy of order table 
# def prepOrder(conn, z, s):
# 	print('Prep with z: ' + z + ' s: ' + s)
# 	orderTableName = 'order_s' + s + '_z' + z
# 	dropOrderTable = 'drop table if exists tmp_order;'
# 	createOrderTable = 'create table tmp_order as select * from ' + orderTableName + ' order by random();'
# 	with conn.cursor() as cur:
# 		print('  updating tables')
# 		cur.execute(dropOrderTable)
# 		cur.execute(createOrderTable)

if __name__ == '__main__':
	main()

