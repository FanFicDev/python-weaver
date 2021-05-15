#!/usr/bin/env python
import sys
import tarfile
import io
import hashlib
import psycopg2

from oil import oil
from weaver import Web
import weaver.enc as enc

def main(db: 'psycopg2.connection') -> int:
	if len(sys.argv) != 3:
		raise Exception("expected wid range")

	wid_s = int(sys.argv[1])
	wid_e = int(sys.argv[2])

	maxId = Web.maxId(db)

	if wid_s > maxId:
		return 0
	wid_e = min(wid_e, maxId)

	wid_s_s = str(wid_s).zfill(10)
	wid_e_s = str(wid_e).zfill(10)

	xzfname = f"data_{wid_s_s}_{wid_e_s}.tar.xz"
	if wid_e > maxId:
		xzfname = f"data_{wid_s_s}_{wid_e_s}_partial.tar.xz"

	mfname = f"./manifest_{wid_s_s}_{wid_e_s}.tsv"
	if wid_e > maxId:
		mfname = f"./manifest_{wid_s_s}_{wid_e_s}_partial.tsv"

	ffnLike = 'https://www.fanfiction.net/%'

	with tarfile.open(xzfname, 'w:xz') as xzf:
		# compute manifest
		manifest_s  = 'id\ttimestamp\turl\tlength\tmd5\n'
		for w in Web.fetchIdRange_g(db, wid_s, wid_e, ulike=ffnLike, status=200):
			if w.response is None or len(w.response) < 1:
				continue
			assert(w.url is not None and w.created is not None)

			dec = enc.decode(w.response, w.url)
			if dec is None:
				continue
			html = dec[1]

			ts = int(w.created/1000)
			html = f"<!--\t{ts}\t{w.url}\t-->\n" + html

			h = hashlib.md5(html.encode('utf-8')).hexdigest()
			l = len(html.encode('utf-8'))
			manifest_s += f"{w.id}\t{ts}\t{w.url}\t{l}\t{h}\n"

		# write raw manifest
		with open(mfname, "w") as mf:
			mf.write(manifest_s)

		# save manifest to txz
		s = io.BytesIO(manifest_s.encode('utf-8'))
		ti = tarfile.TarInfo(name=mfname)
		ti.size = len(manifest_s.encode('utf-8'))
		xzf.addfile(tarinfo=ti, fileobj=s)

		# save individual requests to txz
		for w in Web.fetchIdRange_g(db, wid_s, wid_e, ulike=ffnLike, status=200):
			if w.response is None or len(w.response) < 1:
				continue
			assert(w.url is not None and w.created is not None)

			dec = enc.decode(w.response, w.url)
			if dec is None:
				continue
			html = dec[1]

			ts = int(w.created/1000)
			html = f"<!--\t{ts}\t{w.url}\t-->\n" + html

			s = io.BytesIO(html.encode('utf-8'))
			ti = tarfile.TarInfo(name=f"./{w.id}.html")
			ti.mtime = int(w.created//1000)
			ti.size = len(html.encode('utf-8'))
			xzf.addfile(tarinfo=ti, fileobj=s)

	return 0

if __name__ == '__main__':
	with oil.open() as db:
		res = main(db)
	sys.exit(res)

