#!/usr/bin/env python
import sys
import time
import psycopg2
from oil import oil
from weaver import Web, WebScraper, WebQueue
import weaver.enc as enc

def plog(msg: str, fname: str = "./pffn.log") -> None:
	with open(fname, 'a') as f:
		f.write(msg + '\n')
		print(msg)

def prescrape(db: 'psycopg2.connection', scraper: WebScraper, url: str) -> None:
	print(f"url: {url}")
	w = scraper.softScrape(url)
	assert(w.url is not None and w.response is not None)
	print(f"\tresponse size: {len(w.response)}B")
	#print(f"\trequest headers: {w.requestHeaders}")
	#print(f"\tresponse headers: {w.responseHeaders}")

	dec = enc.decode(w.response, w.url)
	if dec is None:
		print("\tunknown encoding")
		return
	print(f"\tencoding: {dec[0]}")
	html = dec[1]
	print(f"\tdecoded size: {len(html)}B")

stripeCount = 1
stripe = 0
baseDelay = None

if len(sys.argv) == 3 or len(sys.argv) == 4:
	stripeCount = int(sys.argv[1])
	stripe = int(sys.argv[2])
	plog(f"stripeCount: {stripeCount}")
	plog(f"stripe: {stripe}")
	if len(sys.argv) == 4:
		baseDelay = float(sys.argv[3])
		plog(f"baseDelay: {baseDelay}")
else:
	raise Exception("expected stripeCount stripe extraDelay?")

with oil.open() as db:
	scraper = WebScraper(db)
	plog('==========')
	plog(f"source: {scraper.source.__dict__}")
	if baseDelay:
		scraper.baseDelay = baseDelay

	while True:
		wq = WebQueue.next(db, stripeCount, stripe)
		if wq is None:
			time.sleep(10)
			continue
		assert(wq.url is not None)
		prescrape(db, scraper, wq.url)
		if len(Web.wcache(db, [wq.url])) == 1:
			wq.dequeue(db)

