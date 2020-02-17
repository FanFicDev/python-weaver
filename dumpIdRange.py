#!/usr/bin/env python
import sys
import psycopg2
from oil import oil
from weaver import Web
import weaver.enc as enc

def main(db: 'psycopg2.connection') -> int:
	if len(sys.argv) != 3:
		raise Exception("expected wid range")

	wid_s = int(sys.argv[1])
	wid_e = int(sys.argv[2])

	some = Web.fetchIdRange(db, wid_s, wid_e)
	for w in some:
		if w.response is None or len(w.response) < 1:
			continue
		assert(w.url is not None)

		dec = enc.decode(w.response, w.url)
		if dec is None:
			continue
		html = dec[1]
		with open(f"./{w.id}.html", "w") as f:
			f.write(html)

	return 0

if __name__ == '__main__':
	with oil.open() as db:
		res = main(db)
	sys.exit(res)

