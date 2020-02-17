#!/usr/bin/env python
import os
import sys
import math
import gzip
import time
import psycopg2
from oil import oil
import oil.util as util
from weaver import Web

def plog(msg: str) -> None:
	print(msg)
	util.logMessage(msg, fname = './d2fs.log', logDir = './')

def main(db: 'psycopg2.connection') -> None:
	baseDir = '/mnt/a2/fanfiction.net/s/'
	urlPrefix = 'https://www.fanfiction.net/s/'

	maxId = Web.maxId(db)
	print(f"maxId: {maxId}")

	roundTo = 100
	overshoot = 20
	start = 0
	end = maxId
	print(end)
	end = int((end + roundTo -1) / roundTo) * roundTo
	print(end)

	if len(sys.argv) == 2:
		start = int(sys.argv[1])
	if len(sys.argv) == 3:
		partCount = int(sys.argv[1])
		partIdx = int(sys.argv[2])
		per = int(math.floor(end / partCount))
		start = per * partIdx - overshoot
		if partIdx == partCount - 1:
			end += overshoot
		else:
			end = per * partIdx + per + overshoot

	print(f"from {start} to {end}")
	blockSize = 100

	fidx = start - blockSize
	dumpedBlockCount = 0
	while fidx < end:
		fidx += blockSize
		eidx = min(fidx + blockSize, end)
		print(f"  doing ids [{fidx}, {eidx})")

		some = Web.fetchIdRange(db, fidx, eidx, ulike='https://www.fanfiction.net/s/%/%')
		for s in some:
			if s.response is None or len(s.response) < 1:
				continue
			assert(s.url is not None and s.created is not None)
			#print(f"{s.url} {len(s.response)}")
			url = s.url
			ts = int(s.created / 1000)
			data = s.response

			fid = url[len(urlPrefix):].split('/')[0]
			cid = url[len(urlPrefix):].split('/')[1]
			fidz = fid.zfill(9)
			spath = '/'.join([fidz[i * 3:i * 3 + 3] for i in range(3)] + [cid])
			#print(f"{url} => {fid} => {fidz} => {spath}")
			fpath = baseDir + spath + f"/{ts}.html.gz"
			#print(fpath)
			os.makedirs(baseDir + spath, exist_ok=True)
			with gzip.open(fpath, 'wb') as f:
				f.write(data)

		if len(some) > 0:
			dumpedBlockCount += 1
			time.sleep(.1)
		if dumpedBlockCount % 100 == 0:
			time.sleep(.4)

if __name__ == '__main__':
	with oil.open() as db:
		main(db)
	sys.exit(0);

