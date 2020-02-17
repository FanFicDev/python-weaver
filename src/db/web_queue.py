from typing import TYPE_CHECKING, Any, Optional
if TYPE_CHECKING:
	import psycopg2
from weaver.db.web import Web

class WebQueue:
	def __init__(self, id_: int = None, created_: int = None, url_: str = None,
			status_: int = None) -> None:
		self.id = id_
		self.created = created_
		self.url = url_
		self.status = status_

	@staticmethod
	def fromRow(row: Any) -> 'WebQueue':
		return WebQueue(
				id_ = int(row[0]),
				created_ = int(row[1]),
				url_ = row[2],
				status_ = row[3],
			)

	@staticmethod
	def next(db: 'psycopg2.connection', stripeCount: int = 1, stripe: int = 0
			) -> Optional['WebQueue']:
		with db, db.cursor() as curs:
			curs.execute('''
				select * from web_queue
				where id %% %s = %s
				order by id asc
				limit 1
				''', (stripeCount, stripe))
			r = curs.fetchone()
			return WebQueue.fromRow(r) if r is not None else None

	@staticmethod
	def queued(db: 'psycopg2.connection', url: str) -> Optional['WebQueue']:
		with db, db.cursor() as curs:
			curs.execute('''
				select * from web_queue
				where url = %s
				order by created desc
				limit 1
				''', (url,))
			r = curs.fetchone()
			return WebQueue.fromRow(r) if r is not None else None

	@staticmethod
	def enqueue(db: 'psycopg2.connection', url: str, status: int = None
			) -> Optional['WebQueue']:
		# unless we're force retrying, if it already exists abort
		if status is None and len(Web.wcache(db, [url])) == 1:
			return None

		wq = WebQueue.queued(db, url)
		if wq is not None:
			return wq

		with db, db.cursor() as curs:
			import time
			curs.execute('''
				insert into web_queue(created, url, status)
				values(%s, %s, %s)
			''', (int(time.time()) * 1000, url, status))
		return WebQueue.queued(db, url)

	def dequeue(self, db: 'psycopg2.connection') -> None:
		if self.id is None or int(self.id) < 1:
			return
		with db, db.cursor() as curs:
			curs.execute('delete from web_queue where id = %s', (self.id,))

	def exists(self, db: 'psycopg2.connection') -> bool:
		with db, db.cursor() as curs:
			curs.execute('''
				select * from web_queue
				where id = %s
				order by created desc
				limit 1
				''', (self.id,))
			r = curs.fetchone()
			return (r is not None)

