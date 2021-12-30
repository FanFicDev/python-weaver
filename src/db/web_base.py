from typing import TYPE_CHECKING, Any, Optional
if TYPE_CHECKING:
	import psycopg2
from oil.util import uncompress

class WebBase:
	def __init__(self, id_: int = None, created_: int = None,
			encoding_: int = None, response_: bytes = None) -> None:
		self.id = id_
		self.created = created_
		self.encoding = encoding_
		self.response = response_

	@staticmethod
	def fromRow(row: Any) -> 'WebBase':
		return WebBase(
				id_ = row[0],
				created_ = row[1],
				encoding_ = row[2],
				response_ = None if row[3] is None else uncompress(row[3].tobytes()),
			)

	@staticmethod
	def lookup(db: 'psycopg2.connection', wbaseId: int) -> Optional['WebBase']:
		with db.cursor() as curs:
			curs.execute('''
				select * from web_base wb
				where wb.id = %s
			''', (wbaseId,))
			wbs = [WebBase.fromRow(r) for r in curs]
			if len(wbs) == 0:
				return None
			if len(wbs) == 1:
				return wbs[0]
			raise Exception(f"unexpected {len(wbs)} rows")

