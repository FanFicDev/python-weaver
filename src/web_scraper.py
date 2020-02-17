from typing import TYPE_CHECKING, Optional
if TYPE_CHECKING:
	import psycopg2
import time
from oil.util import logMessage, getFuzz
import weaver.cm as cm
import weaver.enc as enc
from weaver.db import WebSource, Web, Encoding

defaultUserAgent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.83 Safari/537.36'
defaultRequestTimeout = 5

class WebScraper:
	def __init__(self, db: 'psycopg2.connection') -> None:
		import os
		self.db = db
		self.staleOnly = False
		if 'HERMES_STALE' in os.environ:
			self.staleOnly = True
		global defaultUserAgent
		self.userAgent = defaultUserAgent
		self.baseDelay: float = 30.0
		self.cookies = cm.getDefaultCookies()
		self.headers = { 'User-Agent': self.userAgent }
		self.source = WebSource.fromEnvironment(self.db)
		if not self.source.isLocal():
			self.baseDelay = 6.0
		self.staleThreshold = 60 * 60 * 8
		self.mustyThreshold: Optional[int] = None

	@staticmethod
	def canonize(url: str) -> str:
		protocol = url[:url.find('://')]
		rest = url[url.find('://') + 3:]
		rest = rest.replace('//', '/')
		# TODO: are there more of these? better way to handle?
		#if rest.endswith('/') and rest.find('phoenixsong.net') == -1:
		#	rest = rest[:-1]
		return protocol + '://' + rest

	def resolveRedirects(self, url: str) -> str:
		import requests
		global defaultRequestTimeout
		url = WebScraper.canonize(url)
		r = requests.get(url, headers=self.headers, cookies=self.cookies,
				timeout=defaultRequestTimeout)
		time.sleep(getFuzz())
		return r.url

	def softScrape(self, url: str, delay: float = None, ulike: str = None) -> Web:
		if delay is None:
			delay = self.baseDelay
		url = WebScraper.canonize(url)
		w = Web.latest(self.db, url, ulike)

		musty = (self.mustyThreshold is not None) \
				and (w is not None and w.created is not None) \
				and ((int(time.time()) - w.created / 1000) > self.mustyThreshold)

		stale = False
		if w is None:
			w = self.scrape(url)
			time.sleep(delay)
			#w = Web.latest(self.db, url, ulike)
		elif w.created is not None:
			stale = (int(time.time()) - w.created / 1000) > self.staleThreshold
			#logMessage('softScrape|{}'.format(url), 'scrape.log')

		# TODO: it would be nice if we could tell mypy that
		#   stale implies w is not None
		# if the scrape is stale and there was a server error, try rescraping it
		#   (or if it's simply really old and we have such a threshold)
		if musty:
			logMessage(f'softScrape|musty|{url}')
		if (stale and w.status is not None and w.status >= 500) \
				or musty:
			w = self.scrape(url)
			time.sleep(delay)
			#w = Web.latest(self.db, url, ulike)

		return w

	def scrape(self, url: str) -> Web:
		if self.staleOnly:
			logMessage(f'staleScrape|{url}', 'scrape.log')
			wl = Web.latest(self.db, url)
			if wl is None:
				raise Exception(f'failed to stale scrape url: {url}')
			return wl

		logMessage(f'scrape|{url}', 'scrape.log')
		created = int(time.time()) * 1000
		w = Web(created_=created, url_=url, sourceId_=self.source.id)

		try:
			import requests
			global defaultRequestTimeout
			r = requests.get(url, headers=self.headers, cookies=self.cookies,
					timeout=defaultRequestTimeout)
			w.status = r.status_code
			w.response = (r.content)
			w.requestHeaders = str(r.request.headers).encode('utf-8')
			w.responseHeaders = str(r.headers).encode('utf-8')
		except:
			logMessage(f'scrape|exception|{url}', 'scrape.log')
			raise

		fuzz = getFuzz()
		# subtract out request time from fuzz
		fuzz -= (int(time.time() * 1000) - created) / 1000
		# TODO: delay *before* scrape based on domain
		time.sleep(max(fuzz, .1) + getFuzz(0.01, 0.1))

		self.last_ts = created

		if w.status != 200:
			w.save(self.db)
			raise Exception(f'failed to download url {w.url}: {w.status}')

		dec = enc.decode(w.response, url)
		if dec is not None and dec[0] is not None:
			w.encoding = Encoding.lookup(self.db, dec[0]).id

		w.save(self.db)
		return w

