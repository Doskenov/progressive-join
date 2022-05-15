# !/bin/python
from fileinput import filename
import psycopg2
import re
from time import time
import csv 
import sys

def main():

	# python filename
	# sigma = float(sys.argv[1])
	filename = str(sys.argv[1])

	joinQueries = [ "select o_orderkey, l_orderkey from orderslg join lineitemlg on o_orderkey = l_orderkey",
					"select p_partkey, l_partkey from partlg, lineitemlg where p_partkey = l_partkey;",
					"select s_suppkey, l_suppkey from supplierlg, lineitemlg where s_suppkey= l_suppkey;"]

		
	conn = psycopg2.connect(host="/tmp/", database="popowskm", user="popowskm", port="5450")
	f = open(filename, 'w+')
	# f.write("connect successfully",conn)

	loop = 1 
	for joinQuery in joinQueries:
		for _ in range(loop):
			timeForKs = measureTimeForKs(conn, joinQuery, f)
	f.close()


# measures the running time of a join query for a given value of k 
def measureTimeForKs(conn, joinQuery, f):
	cur = conn.cursor()
	f.write("Progressive-Merge Join")
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
	f.write(joinQuery)
	# f.write("start time",start)
	cur.execute(joinQuery)
	f.write('  time before fetch: %f sec' % (time() - start))
	fetched = int(0)
	start = time()
	# prev = start
	# factor = sigma
	# weightedTime = 0
	res = list()
	barrier = int(2)
	for _ in cur:
		fetched += 1
		# weightedTime += (current-prev)*factor
		# prev = current
		# factor *= sigma
		if fetched == barrier:
			barrier *= 2
			joinTime = time() - start
			f.write("%d,%f\n"%(fetched,joinTime))
	joinTime = time() - start
	f.write("%d,%f\n"%(fetched,joinTime))
	f.write('  time %.2f sec' % (joinTime))
	res.append(joinTime * 1000)
	f.write("%f"%joinTime)
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

