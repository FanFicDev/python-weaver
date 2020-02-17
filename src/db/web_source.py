from typing import TYPE_CHECKING
if TYPE_CHECKING:
	import psycopg2

_localPrefix = '255.255.255.255' # TODO?

class WebSource:
	def __init__(self, id_: int = None, name_: str = None, source_: str = None,
			description_: str = None) -> None:
		self.id = id_
		self.name = name_
		self.source = source_
		self.description = description_

	def isLocal(self) -> bool:
		return self.source is None or self.source.startswith(_localPrefix)

	@staticmethod
	def create(db: 'psycopg2.connection', name: str, source: str) -> 'WebSource':
		created = False
		with db, db.cursor() as curs:
			curs.execute('''
				insert into web_source(name, source) values(%s, %s) returning id
			''', (name, source))
			row = curs.fetchone()
			created = row is not None
		if created:
			return WebSource.lookup(db, name, source)
		raise Exception(f"unable to create create source {name}:{source}")

	@staticmethod
	def lookup(db: 'psycopg2.connection', name: str, source: str) -> 'WebSource':
		with db, db.cursor() as curs:
			curs.execute('''
				select * from web_source ws where ws.name = %s and ws.source = %s
			''', (name, source))
			row = curs.fetchone()
			if row is not None:
				return WebSource(
						id_ = int(row[0]),
						name_ = str(row[1]),
						source_ = str(row[2]),
						description_ = str(row[3]),
					)
		return WebSource.create(db, name, source)

	@staticmethod
	def fromEnvironment(db: 'psycopg2.connection') -> 'WebSource':
		import os
		src = None
		if 'OIL_SCRAPE_SOURCE' in os.environ:
			src = os.environ['OIL_SCRAPE_SOURCE']
		else:
			from oil.util import lookupRemoteIP
			src = lookupRemoteIP()

		name = 'minerva' # TODO: from env?
		return WebSource.lookup(db, name, src)

