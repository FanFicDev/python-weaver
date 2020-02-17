#!/usr/bin/env python
import sys
from oil import oil
from weaver import WebQueue

with oil.open() as db:
	for line in sys.stdin:
		url = line.strip()
		if not url.startswith('http'):
			raise Exception(f"what: {url}")
		WebQueue.enqueue(db, url)

