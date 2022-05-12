#!/bin/python
import psycopg2
import re
from time import sleep, time
import csv 
import sys

def main():

    # python filename jointype zvalue sigma
    # jointype: 1, Bandit; 0, Block
    t = int(sys.argv[1])
    z = int(sys.argv[2])
    sigma = float(sys.argv[3])

    zValues = [z]
    kValues = [60001]   # 1% of full join


    #joinQueryz1 = 'select * from ordersz11g join tmpz11g_lineitem on o_orderkey = l_orderkey;'
    #joinQueryz2 = 'select * from ordersz21g join tmpz21g_lineitem on o_orderkey = l_orderkey;'
    joinQuery = 'select * from orders2_s0_5_z0 join lineitem_s0_5_z2 on o_orderkey = l_orderkey;'

    #if z == 1:
    #    joinQuery = joinQueryz1
    #elif z == 2:
    #    joinQuery = joinQueryz2
    #elif z == 3:
    #    joinQuery = joinQueryz3  

    if t == 0: 
        join = False
    else: 
        join = True
        
    print(joinQuery)
    # with psycopg2.connect(host="localhost", database="xiemian", user="xiemia", port="5444") as conn:
    conn = psycopg2.connect(host="/tmp/", database="popowskm", user="popowskm", port="5450")
    print("connect successfully",conn)
    loop = 1 
    for z in zValues:
        # banditSum = [0.0] * len(kValues)
        # nestedSum = [0.0] * len(kValues)
        for _ in range(loop):
            # prep(conn, z, '1') # this line shuffles lineitem table
            timeForKs = measureTimeForKs(conn, kValues, join, joinQuery,sigma)
            # nestedSum = [x + y for x, y in zip(nestedSum, timeForKs)]
            # timeForKs = measureTimeForKs(conn, kValues, False, joinQuery)
            # banditSum = [x + y for x, y in zip(banditSum, timeForKs)]
        # nestedJoinTime[z] = [s / loop for s in nestedSum]
        # banditJoinTime[z] = [s / loop for s in banditSum]
    # print('\n')
    # for z in zValues:
    #     print('z: ' + z)
    #     print('join time')
    #     print('nested:' + str(nestedJoinTime[z]))
    #     print('bandit:' + str(banditJoinTime[z]))
    #     print('==========')
    # for z in zValues:
    #     print(','.join(map(str, nestedJoinTime[z])))
    #     print(','.join(map(str, banditJoinTime[z])))

def prep(conn, z, s):
    prepLineitem(conn, z, s)
    # prepOrder(conn, z, s)

# creates a shuffled copy of lineitem table 
def prepLineitem(conn, z, s):
    print('Prep with z: ' + z + ' s: ' + s)
    tableName  = 'lineitem_s' + s + '_z' + z
    dropLineitemTable = 'drop table if exists tmp_lineitem;'
    createLineitemTable = 'create table tmp_lineitem as select * from ' + tableName  + ' order by random();'
    with conn.cursor() as cur:
        print('  updating lineitem table')
        cur.execute(dropLineitemTable)
        cur.execute(createLineitemTable)

# creates a shuffled copy of order table 
def prepOrder(conn, z, s):
    print('Prep with z: ' + z + ' s: ' + s)
    orderTableName = 'order_s' + s + '_z' + z
    dropOrderTable = 'drop table if exists tmp_order;'
    createOrderTable = 'create table tmp_order as select * from ' + orderTableName + ' order by random();'
    with conn.cursor() as cur:
        print('  updating tables')
        cur.execute(dropOrderTable)
        cur.execute(createOrderTable)

# measures the running time of a join query for a given value of k 
def measureTimeForKs(conn, kValues, useBanditJoin, joinQuery,sigma):
    cur = conn.cursor()
    # with conn.cursor() as cur:
    if (useBanditJoin):
        print("Bandit Join")
        cur.execute('set enable_material=off;')
        cur.execute('set max_parallel_workers_per_gather=0;')
        cur.execute('set enable_hashjoin=off;')
        cur.execute('set enable_mergejoin=off;')
        cur.execute('set enable_indexonlyscan=off;')
        cur.execute('set enable_indexscan=off;')
        cur.execute('set enable_block=on;')
        cur.execute('set enable_bitmapscan=off;')
        # cur.execute('set work_mem="64kB";')
        cur.execute('set enable_fastjoin=on;')
        cur.execute('set enable_seqscan=on;')
        cur.execute('set enable_fliporder=off;')
    else:
        print("Block Nest Loop Join")
        cur.execute('set enable_material=off;')
        cur.execute('set max_parallel_workers_per_gather=0;')
        cur.execute('set enable_hashjoin=off;')
        cur.execute('set enable_mergejoin=off;')
        cur.execute('set enable_indexonlyscan=off;')
        cur.execute('set enable_indexscan=off;')
        cur.execute('set enable_block=on;')
        cur.execute('set enable_bitmapscan=off;')
        # cur.execute('set work_mem="64kB";')
        cur.execute('set enable_fastjoin=off;')
        cur.execute('set enable_seqscan=on;')
        cur.execute('set enable_fliporder=off;')
    # with conn.cursor('cur_uniq') as cur:
    # with conn.cursor() as cur:
    cur = conn.cursor('cur_uniq')
    cur.itersize = 1
    start = time()
    print(joinQuery)
    print("start time",start)
    cur.execute(joinQuery)
    print('  time before fetch: %f sec' % (time() - start))
    fetched = 0
    start = time()
    prev = start
    factor = sigma
    weightedTime = 0
    # iteration = 0
    res = list()
    for k in kValues:
        while (fetched < int(k)):
            cur.fetchone()
            # print(queryOutput)
            fetched += 1
            current = time()
            weightedTime += (current-prev)*factor
            prev = current
            factor *= sigma
            if fetched in [20,50,100,500,1000,2000,30000,60000]:
                joinTime = current - start
                print(fetched,joinTime,weightedTime)
                
            # elif fetched % 200 == 0:
            #     print(fetched,joinTime)      
        joinTime = time() - start
        print('  time for k = %s is %.2f sec' % (k, joinTime))
        res.append(joinTime * 1000)
    print(joinTime)

    return res

if __name__ == '__main__':
    main()

