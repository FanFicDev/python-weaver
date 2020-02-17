#!/usr/bin/env python
import tarfile
from oil import oil
from weaver import Web, Encoding, WebSource

total = 0
xzfname = 'min_bulk_dump.tar.xz'

with oil.open() as db:
	source = WebSource.lookup(db, 'iris-bulk', 'iris-bulk')
	encoding = Encoding.lookup(db, 'utf8')

	with tarfile.open(xzfname, 'r:xz') as xzf:
		for ti in xzf:
			total += 1
			if total % 100 == 0:
				print(total)
			fo = xzf.extractfile(ti)
			assert(fo is not None)
			html = str(fo.read().decode('utf-8'))
			header, _, html = html.partition('\n')

			ts = int(header.split('\t')[1])
			url = str(header.split('\t')[2])

			if len(Web.wcache(db, [url])) < 1:
				print(f'  {url}: {ts}')
				w = Web(
						created_ = ts,
						url_ = url,
						status_ = 200,
						sourceId_ = source.id,
						encoding_ = encoding.id,
						response_ = html.encode('utf-8'),
						requestHeaders_ = None,
						responseHeaders_ = None,
						wbaseId_ = None,
					)
				w.save(db)

