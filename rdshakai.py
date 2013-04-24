#!/usr/bin/env python
import umysql

DB_HOST = 'localhost'
DB_USER = 'benchmark'
DB_PASS = 'benchmark'
DB_DB = 'benchmark'
N = 1

def connect(autocommit=True):
    con = umysql.Connection()
    con.connect(DB_HOST, 3306, DB_USER, DB_PASS, DB_DB, autocommit)
    return con

def hakai(id, autocommit=True, n=1000):
    con = connect(autocommit)
    sql = "update bench set val=val+1 where id=%d" % (id,)

    for _ in xrange(N):
        con.query(sql)
        if not autocommit:
            con.commit()

    con.close()

def prepare(c):
    con = connect()
    con.query("TRUNCATE TABLE bench")
    for id in xrange(c):
        con.query("insert into bench (id, val) values (%d, 0)" % (id+1,))
    con.close()


def main():
    global N
    import multiprocessing
    import time

    c, N = 1, 1
    import sys
    if len(sys.argv) > 1:
        N = int(sys.argv[1])
    if len(sys.argv) > 2:
        c = int(sys.argv[2])

    prepare(c)
    pool = multiprocessing.Pool(c)
    t = time.time()
    pool.map(hakai, range(1, c+1))
    t = time.time() - t
    print("%s [sec] %s [trx/sec]" % (t, (c*N)/t))

if __name__ == '__main__':
    main()
