#!/usr/bin/env python
import sys
import time
import psycopg2
from bs4 import BeautifulSoup # type: ignore
import oil.util as util
from oil import oil
from weaver import RemoteWebScraper
import weaver.enc as enc

def plog(msg: str, fname: str = "./pr.log") -> None:
	with open(fname, 'a') as f:
		f.write(msg + '\n')
		print(msg)

def prescrape(scraper: RemoteWebScraper, url: str) -> None:
	print(f"url: {url}")
	w = scraper.softScrape(url)
	responseSize = len(w.response) if w.response is not None else 0
	print(f"\tresponse size: {responseSize}B")
	print(f"\trequest headers: {w.requestHeaders!r}")
	print(f"\tresponse headers: {w.responseHeaders!r}")

	dec = enc.decode(w.response, url)
	if dec is None:
		print("\tunknown encoding")
		return
	print(f"\tencoding: {dec[0]}")
	html = dec[1]
	soup = BeautifulSoup(html, 'html5lib')
	print(f"\tdecoded size: {len(html)}B")

def main(db: 'psycopg2.connection') -> int:
	scraper = RemoteWebScraper(db)
	plog('==========')

	for line in sys.stdin:
		try:
			prescrape(scraper, line.strip())
			time.sleep(1)
		except SystemExit as e:
			raise
		except:
			pass

	return 0

if __name__ == '__main__':
	with oil.open() as db:
		res = main(db)
	sys.exit(res)

