from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
	import psycopg2
import time
from oil.util import logMessage
import weaver.cm as cm
import weaver.enc as enc
from weaver.db import Web, WebQueue, Encoding
from weaver import WebScraper

# FIXME this should probably actually subclass WebScraper and rely on it for
# several pieces here

class RemoteWebScraper:
	def __init__(self, db: 'psycopg2.connection') -> None:
		import os
		self.db = db
		self.staleOnly = False
		if 'HERMES_STALE' in os.environ:
			self.staleOnly = True
		self.staleThreshold = 60 * 60 * 8
		self.mustyThreshold: Optional[int] = None
		self.requestTimeout = 90
		self.spinWaitTime = 0.2

	def softScrape(self, url: str, ulike: str = None) -> Web:
		url = WebScraper.canonize(url)
		w = Web.latest(self.db, url, ulike)

		musty = (self.mustyThreshold is not None) \
				and (w is not None and w.created is not None) \
				and ((int(time.time()) - w.created / 1000) > self.mustyThreshold)

		stale = False
		if w is None:
			w = self.scrape(url)
		elif w.created is not None:
			stale = (int(time.time()) - w.created / 1000) > self.staleThreshold

		# TODO: it would be nice if we could tell mypy that
		#   stale implies w is not None
		# if the scrape is stale and there was a server error, try rescraping it
		#   (or if it's simply really old and we have such a threshold)
		if musty:
			logMessage(f'softScrape|musty|{url}')
		if (stale and w.status is not None and w.status >= 500) \
				or musty:
			w = self.scrape(url)

		return w

	def enqueue(self, url: str, priority: int = None) -> WebQueue:
		stale = 0
		if self.staleThreshold is not None:
			stale = int((time.time() - self.staleThreshold) * 1000)
		musty = 0
		if self.mustyThreshold is not None:
			musty = int((time.time() - self.mustyThreshold) * 1000)

		# FIXME what are all these params?
		wq = WebQueue.enqueue(self.db, url, stale, musty, 1, priority)
		if wq is None:
			raise Exception(f'failed to queue url: {url}')
		return wq

	def scrape(self, url: str) -> Web:
		if self.staleOnly:
			logMessage(f'staleScrape|{url}', 'scrape.log')
			wl = Web.latest(self.db, url)
			if wl is None:
				raise Exception(f'failed to stale scrape url: {url}')
			return wl

		wq = self.enqueue(url)

		totalWait = 0.0
		while totalWait < self.requestTimeout and wq.exists(self.db):
			time.sleep(self.spinWaitTime)
			totalWait += self.spinWaitTime

		if wq.exists(self.db):
			raise Exception(f'remote scrape timed out: {url}')

		w = Web.latest(self.db, url)
		if w is None:
			raise Exception(f'failed to scrape url: {url}')
		return w

