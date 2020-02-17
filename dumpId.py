#!/usr/bin/env python
import sys
import psycopg2
from oil import oil
from weaver import Web
import weaver.enc as enc

def main(db: 'psycopg2.connection') -> int:
	if len(sys.argv) != 2:
		raise Exception("expected wid")

	wid = int(sys.argv[1])

	some = Web.fetchIdRange(db, wid, wid + 1)
	if len(some) != 1:
		raise Exception("TODO")

	w = some[0]
	if w.response is None or len(w.response) < 1:
		return 0
	assert(w.url is not None)

	dec = enc.decode(w.response, w.url)
	if dec is None:
		raise Exception("unknown encoding")
	html = dec[1]
	print(html)

	return 0

if __name__ == '__main__':
	with oil.open() as db:
		res = main(db)
	sys.exit(res)

