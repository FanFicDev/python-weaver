from typing import TYPE_CHECKING, Optional, Any, Iterator, List, Set, Iterable
if TYPE_CHECKING:
	import psycopg2
from oil.util import compress, uncompress, getUniqueJobName

class Web:
	def __init__(self, id_: int = None, created_: int = None, url_: str = None,
			status_: int = None, sourceId_: int = None, encoding_: int = None,
			response_: bytes = None, requestHeaders_: Optional[bytes] = None,
			responseHeaders_: Optional[bytes] = None, wbaseId_: Optional[int] = None):
		self.id = id_
		self.created = created_
		self.url = url_
		self.status = status_
		self.sourceId = sourceId_
		self.encoding = encoding_
		self.response = response_
		self.requestHeaders = requestHeaders_
		self.responseHeaders = responseHeaders_
		self.wbaseId = wbaseId_

	@staticmethod
	def maxId(db: 'psycopg2.connection') -> int:
		with db, db.cursor() as curs:
			curs.execute('''
				select max(w.id) from web w
			''')
			r = curs.fetchone()
			return int(r[0]) if r is not None else -1

	@staticmethod
	def fromRow(row: Any) -> 'Web':
		return Web(
				id_ = row[0],
				created_ = row[1],
				url_ = row[2],
				status_ = row[3],
				sourceId_ = row[4],
				encoding_ = row[5],
				response_ = None if row[6] is None else uncompress(row[6].tobytes()),
				requestHeaders_ = None if row[7] is None \
						else uncompress(row[7].tobytes()),
				responseHeaders_ = None if row[8] is None \
						else uncompress(row[8].tobytes()),
				wbaseId_ = row[9],
			)

	@staticmethod
	def fetchIdRange_g(db: 'psycopg2.connection', start: int, end: int,
			ulike: str = None, status: int = 200) -> Iterator['Web']:
		with db, db.cursor(getUniqueJobName('Web.fetchIdRange_g')) as curs:
			curs.execute('''
				select * from web w
				where (%s is null or w.url like %s)
					and (w.id >= %s and w.id < %s)
					and (%s is null or w.status = %s)
				order by w.id asc
			''', (ulike, ulike, start, end, status, status))
			for r in curs:
				yield Web.fromRow(r)

	@staticmethod
	def fetchIdRange(db: 'psycopg2.connection', start: int, end: int,
			ulike: str = None, status: int = 200) -> List['Web']:
		return [w for w in Web.fetchIdRange_g(db, start, end, ulike, status)]

	@staticmethod
	def latest(db: 'psycopg2.connection', url: Optional[str],
			ulike: Optional[str] = None) -> Optional['Web']:
		with db, db.cursor() as curs:
			curs.execute('''
				select * from web w
				where (%s is null or w.url = %s)
					and (%s is null or w.url like %s)
				order by created desc nulls last
			''', (url, url, ulike, ulike))
			r = curs.fetchone()
			if r is None:
				return None
			return Web.fromRow(r)

	@staticmethod
	def latestMany(db: 'psycopg2.connection', urls_: List[str]) -> List['Web']:
		urls = tuple(urls_)
		with db, db.cursor() as curs:
			curs.execute('''
				select w.*
				from web w
				where w.url in %s
					and not exists (
						select 1 from web iw
						where iw.url = w.url and iw.created > w.created
					)
				order by created desc nulls last
			''', (urls,))
			return [Web.fromRow(r) for r in curs.fetchall()]

	@staticmethod
	def wcache(db: 'psycopg2.connection', urls_: Iterable[str],
			status: int = 200) -> Set[str]:
		urls = tuple(urls_)
		with db, db.cursor() as curs:
			curs.execute('''
				select w.url
				from web w
				where w.url in %s
					and w.status = %s
			''', (urls, status))
			return { r[0] for r in curs.fetchall() }

	@staticmethod
	def countFFNNeedsCached(db: 'psycopg2.connection', start: int, end: int,
			cid: int, stripeCount: int, stripe: int, status: int = 200) -> int:
		with db, db.cursor() as curs:
			curs.execute('''
				select count(1)
				from generate_series(%s, %s) n
				where n %% %s = %s
				and not exists (
					select 1 from web w
					where w.url = 'https://www.fanfiction.net/s/' || n || '/' || %s
						and w.status = %s
				)
			''', (start, end, stripeCount, stripe, cid, status))
			r = curs.fetchone()
			if r is None:
				raise Exception(f"no results to select count?")
			return int(r[0])

	def _save(self, db: 'psycopg2.connection') -> int:
		with db.cursor() as curs:
			curs.execute('''
				insert into web(created, url, status, sourceId, encoding, response,
					requestHeaders, responseHeaders)
				values(%s, %s, %s, %s, %s, %s, %s, %s)
				returning id
			''', (self.created, self.url, self.status, self.sourceId, self.encoding,
				None if self.response is None else compress(self.response),
				None if self.requestHeaders is None else compress(self.requestHeaders),
				None if self.responseHeaders is None else compress(self.responseHeaders)))
			r = curs.fetchone()
			if r is None:
				raise Exception(f"failed to insert?")
			self.id = int(r[0])
			return self.id

	def save(self, db: 'psycopg2.connection', trans: bool = True) -> int:
		if self.id is not None:
			raise Exception('Web only supports insert')
		if trans:
			with db:
				return self._save(db)
		return self._save(db)

