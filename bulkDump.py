#!/usr/bin/env python
import sys
import io
import tarfile
from typing import List, Iterable
from oil import oil
from weaver import Web
import weaver.enc as enc

def chunkList(l: List, cnt: int) -> Iterable[List]:
	for i in range(0, len(l), cnt):
		yield l[i:i + cnt]

urls = [line.strip() for line in sys.stdin]
print(len(urls))
totalLen = 0
totalChunks = 0

xzfname = 'min_bulk_dump.tar.xz'

with oil.open() as db:
	with tarfile.open(xzfname, 'w:xz') as xzf:
		for chunk in chunkList(urls, 1000):
			totalChunks += 1
			print(totalChunks)
			for w in Web.latestMany(db, chunk):
				assert(w.url is not None and w.created is not None)
				dec = enc.decode(w.response, w.url)
				if dec is None:
					continue
				html = dec[1]
				totalLen += len(html)

				ts = int(w.created/1000)
				html = f"<!--\t{ts}\t{w.url}\t-->\n" + html

				s = io.BytesIO(html.encode('utf-8'))
				ti = tarfile.TarInfo(name=f"./{w.id}.html")
				ti.size = len(html.encode('utf-8'))
				xzf.addfile(tarinfo=ti, fileobj=s)

print(totalLen)

