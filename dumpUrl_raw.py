#!/usr/bin/env python
import sys
from oil import oil
from weaver import WebScraper
import weaver.enc as enc

def prescrape(scraper: WebScraper, url: str) -> None:
	w = scraper.softScrape(url)
	assert(w.url is not None)
	dec = enc.decode(w.response, w.url)
	if dec is None:
		raise Exception("unknown encoding")
	html = dec[1]
	print(html)

with oil.open() as db:
	scraper = WebScraper(db)
	scraper.baseDelay = 30

	for line in sys.stdin:
		try:
			prescrape(scraper, line.strip())
		except:
			pass

