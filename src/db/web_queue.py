from typing import TYPE_CHECKING, Any, Optional
if TYPE_CHECKING:
	import psycopg2
from weaver.db.web import Web

class WebQueue:
	# TODO we really need to document the intent here
	def __init__(self, id_: int = None, created_: int = None, url_: str = None,
			status_: int = None, workerId_: int = None, touched_: int = None,
			stale_: int = None, musty_: int = None, priority_: int = None) -> None:
		self.id = id_
		self.created = created_
		self.url = url_
		self.status = status_
		self.workerId = workerId_
		self.touched = touched_
		self.stale = stale_
		self.musty = musty_
		self.priority = priority_

	@staticmethod
	def fromRow(row: Any) -> 'WebQueue':
		return WebQueue(
				id_ = int(row[0]),
				created_ = int(row[1]),
				url_ = row[2],
				status_ = row[3],
				workerId_ = row[4],
				touched_ = row[5],
				stale_ = row[6],
				musty_ = row[7],
				priority_ = row[8],
			)

	@staticmethod
	def resetWorker(db: 'psycopg2.connection', workerId: int) -> None:
		with db, db.cursor() as curs:
			curs.execute('''
				update web_queue
				set workerId = null, touched = oil_timestamp()
				where workerId = %s
				''', (workerId,))

	@staticmethod
	def next(db: 'psycopg2.connection', workerId: int, stale: int = (45 * 1000),
			stripeCount: int = 1, stripe: int = 0) -> Optional['WebQueue']:
		with db, db.cursor() as curs:
			curs.execute('''
				update web_queue wq
				set workerId = %s, touched = oil_timestamp()
				where wq.id = (
					select id from web_queue
					where id %% %s = %s
						and (workerId is null
							or touched is null
							or touched < oil_timestamp() - %s)
					order by priority desc nulls last, id asc
					for update skip locked
					limit 1
				) and (wq.workerId is null
						or wq.touched is null
						or wq.touched < oil_timestamp() - %s)
				returning wq.*
				''', (workerId, stripeCount, stripe, stale, stale))
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
	def enqueue(db: 'psycopg2.connection', url: str, stale: int, musty: int,
			status: int = None, priority: int = None) -> Optional['WebQueue']:
		# unless we're force retrying, if it already exists abort
		if (status is None and musty == 0) and len(Web.wcache(db, [url])) == 1:
			return None

		# TODO trying to skip requeue is complicated...

		with db, db.cursor() as curs:
			import time
			curs.execute('''
				insert into web_queue(created, url, status, stale, musty, priority)
				values(%s, %s, %s, %s, %s, %s)
				returning  *
			''', (int(time.time()) * 1000, url, status, stale, musty, priority))
			r = curs.fetchone()
			return WebQueue.fromRow(r)

		raise Exception(f'WebQueue.enqueue: failed to queue: {url}')

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

