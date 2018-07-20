#!/usr/bin/env python
#coding:utf-8
'''参数说明:
此工具只适用于值类型为字符串的操作，如需操作其它类型可以在此基础上更改：

$ python redis-dump.py dump "源主机IP:源端口:db0:密码" "key匹配pattern" "导出josn文件名"
$ python redis-dump.py load "目的主机IP:目的端口:db0:密码" "导入josn文件名"
$ python redis-dump.py sync "源主机IP:源端口:db0:密码" "目的主机IP:目的端口:db0:密码" "key匹配pattern" 
如:
$ python redis-dump.py dump "10.0.0.1:6380:db0:PASSWORD" "ab:*" "/tmp/ab.json"
'''
import redis
import json
import sys

def dump():
    args = sys.argv[2].split(":")
    sIP = args[0]
    sPort= int(args[1])
    sDB = int(args[2].lower().replace('db',''))
    sPasswd = args[3]

    kPatt = sys.argv[3]
    dumpfile = sys.argv[4]
    
    rdb = redis.StrictRedis(host=sIP,port=sPort,db=sDB,password="%s"%sPasswd)

    #print args,kPatt,dumpfile
    
    res = {}
    for i in rdb.keys(kPatt):
        item =  {i : {'ttl': rdb.ttl(i), "value": rdb.get(i)}}
        res.update(item)
        print item
    open(dumpfile,'wb').write(json.dumps(res))

def load():
    args = sys.argv[2].split(":")
    tIP = args[0]
    tPort= int(args[1])
    tDB = int(args[2].lower().replace('db',''))
    tPasswd = args[3]

    dumpfile = sys.argv[3]
    
    rdb = redis.StrictRedis(host=tIP,port=tPort,db=tDB,password="%s"%tPasswd)

    #print args,dumpfile
    
    res = json.loads(open(dumpfile,'rb').read())
    for i in res:
        rdb.set(i,res[i]['value'])
        if res[i]['ttl'] > 0:
            rdb.expire(i,res[i]['ttl'])
        print {i : res[i]}

def sync():
    sargs = sys.argv[2].split(":")
    sIP = sargs[0]
    sPort= int(sargs[1])
    sDB = int(sargs[2].lower().replace('db',''))
    sPasswd = sargs[3]
    
    targs = sys.argv[3].split(":")
    tIP = targs[0]
    tPort= int(targs[1])
    tDB = int(targs[2].lower().replace('db',''))
    tPasswd = targs[3]
        
    kPatt = sys.argv[4]
    
    rdb1 = redis.StrictRedis(host=sIP,port=sPort,db=sDB,password="%s"%sPasswd)
    rdb2 = redis.StrictRedis(host=tIP,port=tPort,db=tDB,password="%s"%tPasswd)
    
    for i in rdb1.keys(kPatt):
        value = rdb1.get(i)
        ttl = rdb1.ttl(i)
        rdb2.set(i, value)
        if ttl > 0:
            rdb2.expire(i, ttl)
        print {i : {"ttl": ttl , "value": value }}

if __name__ == "__main__":
    if len(sys.argv) == 1:
        print __doc__
    elif sys.argv[1] == 'dump':
        dump()
    elif sys.argv[1] == 'load':
        load()
    elif sys.argv[1] == 'sync':
        sync()
    else:
        print __doc__
