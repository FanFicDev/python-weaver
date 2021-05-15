#!/usr/bin/env python
import sys
import time
import platform
from typing import Optional
import psycopg2
from oil import oil
from weaver import Web, WebScraper, WebQueue
import weaver.enc as enc

def plog(msg: str, fname: str = "./pffn.log") -> None:
	with open(fname, 'a') as f:
		f.write(msg + '\n')
		print(msg)

def prescrape(scraper: WebScraper, wq: WebQueue) -> Optional[Web]:
	assert(wq.url is not None)
	print(f"url: {wq.url}")
	w = scraper.softScrape(wq.url)
	assert(w.created is not None)
	#print(f"  {w.created} {wq.musty}")
	if wq.musty is not None and w.created < wq.musty:
		print(f"  musty, rescraping")
		w = scraper.scrape(wq.url)
	assert(w.url is not None and w.response is not None)
	print(f"\tresponse size: {len(w.response)}B")
	#print(f"\trequest headers: {w.requestHeaders}")
	#print(f"\tresponse headers: {w.responseHeaders}")

	dec = enc.decode(w.response, w.url)
	if dec is None:
		print("\tunknown encoding")
		return None
	print(f"\tencoding: {dec[0]}")
	html = dec[1]
	print(f"\tdecoded size: {len(html)}B")
	return w

NODE_PREFIX = 'minerva'
node = platform.node()
if not node.startswith(NODE_PREFIX) \
		or not node[len(NODE_PREFIX):].isnumeric():
	plog(f"err: node {node} is not {NODE_PREFIX}[workerId]")
	raise Exception("expected valid node name")

workerId = int(node[len(NODE_PREFIX):])
plog(f"workerId: {workerId}")

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

	# we handle sleeping in our loop
	loopDelay = scraper.baseDelay
	scraper.baseDelay = 0.01

	while True:
		wq = WebQueue.next(db, workerId, stripeCount=stripeCount, stripe=stripe)
		if wq is None:
			time.sleep(.05)
			continue
		assert(wq.url is not None)
		w = prescrape(scraper, wq)
		if len(Web.wcache(db, [wq.url])) == 1:
			wq.dequeue(db)
		if w is not None:
			assert(w.created is not None)
			if w.created > int((time.time() - 30) * 1000):
				time.sleep(loopDelay)
		else:
			time.sleep(loopDelay)

