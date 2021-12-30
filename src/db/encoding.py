from typing import TYPE_CHECKING
if TYPE_CHECKING:
	import psycopg2

class Encoding:
	def __init__(self, id_: int = None, name_: str = None) -> None:
		self.id = id_
		self.name = name_

	@staticmethod
	def create(db: 'psycopg2.connection', name: str) -> 'Encoding':
		with db.cursor() as curs:
			curs.execute('insert into encoding(name) values(%s) returning id', (name,))
			row = curs.fetchone()
			if row is not None:
				return Encoding.lookup(db, name)
			raise Exception(f"unable to create create encoding {name}")

	@staticmethod
	def lookup(db: 'psycopg2.connection', name: str) -> 'Encoding':
		with db.cursor() as curs:
			curs.execute('select * from encoding e where e.name = %s', (name,))
			row = curs.fetchone()
			if row is None:
				return Encoding.create(db, name)
			return Encoding(
					id_ = int(row[0]),
					name_ = str(row[1]),
				)

