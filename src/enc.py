import traceback
from typing import List, Tuple, Optional
import oil.util as util

decodeFailureDumpFile = '/tmp/enc_decodeFailure.html'

# in many cases we have cp1252 embedded into a page with utf8 on the same
# page; this is used to munge the utf8 into cp1252 so the cp1252 decode
# process won't choke as hard
utf8_to_cp1252: List[Tuple[bytes, bytes]] = []

# this is a set of cleanup transforms on (an assumed) cp1252 input to
# normalize things like smart quotes and delete completely invalid chars
cp1252_munge: List[Tuple[bytes, bytes]] = []

def setupCP1252() -> None:
	global cp1252_munge, utf8_to_cp1252
	if len(cp1252_munge) > 0 and len(utf8_to_cp1252) > 0:
		return
	utf8_to_cp1252 = [
			(b'\xc2\xa9', b'\xa9'), # ©
			(b'\xc2\xb0', b'\xb0'), # °
			(b'\xc3\xa1', b'\xe1'), # á
			(b'\xc3\xa7', b'\xe7'), # ç
			(b'\xc3\xa8', b'\xe8'), # è
			(b'\xc3\xa9', b'\xe9'), # é
			(b'\xc3\xa0', b'\xe0'), # à
			(b'\xe2\x80\xa6', b'\x85'), # …
			(b'\xe2\x80\x93', b'\x96'), # –
			(b'\xe2\x80\x99', b'\x92'), # ’
			(b'\xe2\x80\x9c', b'\x93'), # “
			(b'\xe2\x80\x9d', b'\x94'), # ”
			(b'\xef\xbf\xbd', b'\x81'), # literal question mark block >_>
		]
	# FIXME these are weird:
	#   \x98 ˜    \xa6 ¦
	cp1252_munge = [
			(b'\x81', b''), # invalid
			(b'\x91', b"'"), # ‘
			(b'\x92', b"'"), # ’
			(b'\x93', b'"'), # “
			(b'\x94', b'"'), # ”
			(b'\x96', b'-'), # –
			(b'\x97', b'-'), # —
			(b'\x9d', b''), # undefined
			(b'\xa0', b' '), # nbsp
			(b'\xad', b''), # soft hyphen
		]

# attempt to decode the bytes in data as utf8 and then as munged cp1252
# returns (encoding, decoded str)
# returns None if something goes wrong
def decode(data: Optional[bytes], url: str) -> Optional[Tuple[str, str]]:
	global decodeFailureDumpFile
	if data is None:
		return None

	try:
		dec = data.decode('utf-8')
		return ('utf8', dec)
	except:
		pass

	setupCP1252()

	# handle Mórrigan and façade in
	# http://www.fictionalley.org/authors/irina/galatea05.html
	# looks aggressively misencoded
	data = \
		data.replace(b'M\xc3\x83\xc2\xb3rr\xc3\x83\xc2\xadgan',
			b'M\xf3rrigan')
	data = \
		data.replace(b'fa\xc3\x83\xc2\xa7ade', b'fa\xe7ade')

	data = data.replace(b'#8211;&#8212;&#8211;\xb5&#8211;\xbb&#8211;\xb8',
			b'#8211;&#8212;&#8211;&#8211;&#8211;')
	data = data.replace(b'#8211;&#8211;&#8211;\xb9 &#8211; &#8212;\x83',
			b'#8211;&#8211;&#8211; &#8211; &#8212;')
	data = data.replace(b'&#8211;\xb9 &#8211; &#8212;\x83',
			b'#8211;&#8211;&#8211; &#8211; &#8212;&#8')

	# replace misencoded utf-8 bits (likely from a header or footer) with their
	# cp1252 counterparts
	for utoc in utf8_to_cp1252:
		data = data.replace(utoc[0], utoc[1])

	# do some cleanup on the remaining cp1252 to normalize smart quotes and
	# delete a few invalid chars that may have leaked through
	for ctom in cp1252_munge:
		data = data.replace(ctom[0], ctom[1])

	try:
		dec = data.decode('cp1252')
		return ('cp1252', dec)
	except Exception as e:
		util.logMessage('error decoding {}: {}\n{}'.format(url, e, traceback.format_exc()))

	with open(decodeFailureDumpFile, 'wb') as f:
		f.write(data)
	return None

