#!/usr/bin/env python
import time
import random
import statistics
import psycopg2
from typing import List
from oil import oil
from weaver import Web

oldMaxId = 0
maxId = 13498300 - 100000
blockSize = 1000

def getUrl(fid: int, cid: int = 1) -> str:
	return f"https://www.fanfiction.net/s/{fid}/{cid}"

def testBlock(db: 'psycopg2.connection', start: int, end: int, cid: int
		) -> float:
	urls = [getUrl(fid, cid) for fid in range(start, end)]

	s1 = time.time()
	Web.wcache(db, urls)
	e1 = time.time()

	s2 = time.time()
	Web.latestMany(db, urls)
	e2 = time.time()

	t1 = e1 - s1
	t2 = e2 - s2

	print(f"{t1} {t2}: {t2 / t1}")
	return t2/t1

blockIdxs = [idx for idx in
		range(max(int(oldMaxId / blockSize) - 20, 0), int(maxId / blockSize) + 1)]
random.shuffle(blockIdxs)

count=0
mults: List[float] = []
with oil.open() as db:
	for idx in blockIdxs:
		count += 1
		mults += [testBlock(db, idx * blockSize, (idx + 1) * blockSize, 1)]
		if count > 5:
			break

print(mults)
print(statistics.mean(mults))

