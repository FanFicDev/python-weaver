import traceback
from typing import List, Tuple, Optional
import oil.util as util

decodeFailureDumpFile = '/tmp/enc_decodeFailure.html'

# this is a set of transforms for turning cp1252 values into their utf8
# equivalent or an empty bytestring
cp1252: List[Tuple[bytes, bytes]] = []

# in many cases we have cp1252 embedded into a page with utf8 on the same
# page; this is used to munge the utf8 into cp1252 so the cp1252 => utf8
# process won't choke as hard
utf8_to_cp1252: List[Tuple[bytes, bytes]] = []

def setupCP1252() -> None:
	global cp1252, utf8_to_cp1252
	if len(cp1252) > 0 and len(utf8_to_cp1252) > 0:
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
	cp1252 = [
			# invalid, convert to literal question mark block
			(b'\x81', b'\xef\xbf\xbd'),

			(b'\x80', b'\xe2\x82\xac'), # €
			(b'\x82', b'\xe2\x80\x9a'), # ‚
			(b'\x83', b'\xc6\x92'), # ƒ
			(b'\x84', b'\xe2\x80\x9e'), # „
			(b'\x85', b'\xe2\x80\xa6'), # …
			(b'\x88', b'\xcb\x86'), # ˆ
			(b'\x8b', b'\xe2\x80\xb9'), # ‹
			(b'\x91', b"'"), # ‘
			(b'\x92', b"'"), # ’
			(b'\x93', b'"'), # “
			(b'\x94', b'"'), # ”
			(b'\x96', b'-'), # –
			(b'\x97', b'-'), # —
			(b'\x99', b'\xe2\x84\xa2'), # ™
			(b'\x9c', b'\xc5\x93'), # œ
			(b'\x9d', b''), # undefined
			# (b'\x9d', b'\xc5\x93'),
			(b'\x9e', b'\xc5\xbe'), # ž
			(b'\xa0', b' '), # nbsp
			(b'\xa7', b'\xc2\xa7'), # §
			(b'\xa8', b'\xc2\xa8'), # ¨
			(b'\xa9', b'\xc2\xa9'), # ©
			(b'\xac', b'\xc2\xac'), # ¬
			(b'\xad', b''), # soft hyphen
			(b'\xb0', b'\xc2\xb0'), # °
			(b'\xb2', b'\xc2\xb2'), # ²
			(b'\xb3', b'\xc2\xb3'), # ³
			(b'\xb4', b'\xc2\xb4'), # ´
			(b'\xb5', b'\xc2\xb5'), # µ
			(b'\xb6', b'\xc2\xb6'), # ¶
			(b'\xb8', b'\xc2\xb8'), # ¸
			(b'\xb9', b'\xc2\xb9'), # ¹
			(b'\xba', b'\xc2\xba'), # º
			(b'\xbc', b'\xc2\xbc'), # ¼
			(b'\xbd', b'\xc2\xbd'), # ½
			(b'\xbe', b'\xc2\xbe'), # ¾
			(b'\xbf', b'\xc2\xbf'), # ¿
			(b'\xc2', b'\xc3\x82'), # Â
			(b'\xc3', b'\xc3\x83'), # Ã
			(b'\xc4', b'\xc3\x84'), # Ä
			(b'\xdb', b'\xc3\x9b'), # Û
			(b'\xdf', b'\xc3\x9f'), # ß
			(b'\xe0', b'\xc3\xa0'), # à
			(b'\xe1', b'\xc3\xa1'), # á
			(b'\xe2', b'\xc3\xa2'), # â
			(b'\xe4', b'\xc3\xa4'), # ä
			(b'\xe6', b'\xc3\xa6'), # æ
			(b'\xe7', b'\xc3\xa7'), # ç
			(b'\xe8', b'\xc3\xa8'), # è
			(b'\xe9', b'\xc3\xa9'), # é
			(b'\xea', b'\xc3\xaa'), # ê
			(b'\xeb', b'\xc3\xab'), # ë
			(b'\xec', b'\xc3\xac'), # ì
			(b'\xed', b'\xc3\xad'), # í
			(b'\xee', b'\xc3\xae'), # î
			(b'\xef', b'\xc3\xaf'), # ï
			(b'\xf1', b'\xc3\xb1'), # ñ
			(b'\xf2', b'\xc3\xb2'), # ò
			(b'\xf3', b'\xc3\xb3'), # ó
			(b'\xf4', b'\xc3\xb4'), # ô
			(b'\xf6', b'\xc3\xb6'), # ö
			(b'\xf8', b'\xc3\xb8'), # ø
			(b'\xfa', b'\xc3\xba'), # ú
			(b'\xfb', b'\xc3\xbb'), # û
			(b'\xfc', b'\xc3\xbc'), # ü
		]

# attempt to decode the bytes in data as utf8 and then as munged cp1252
# returns (encoding, decoded str)
# returns (None, None) if something goes wrong
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


	cm = { c[0]: c[1] for c in cp1252 }
	minBin = int(cp1252[0][0][0])
	for c in cp1252:
		minBin = min(minBin, int(c[0][0]))

	for utoc in utf8_to_cp1252:
		data = data.replace(utoc[0], utoc[1])

	ndata = bytes(''.encode('utf-8'))
	idx = -1
	datalen = len(data)
	while idx < datalen:
		idx += 1
		# if we hit a run of good chars, just copy them wholesale
		eidx = idx
		while eidx < datalen and data[eidx] < minBin:
			eidx += 1
		if eidx > idx:
			ndata += data[idx:eidx]
			idx = eidx - 1
			continue

		if data[idx:idx+1] not in cm:
			ndata += data[idx:idx+1]
			continue
		ndata += cm[data[idx:idx+1]]

	try:
		dec = ndata.decode('utf-8')
		return ('cp1252', dec)
	except UnicodeDecodeError as error:
		util.logMessage('error decoding {}: {}\n{}'.format(url, error, traceback.format_exc()))
		util.logMessage('{}'.format((error.args[0], error.args[2], error.args[3], error.args[4])))
		util.logMessage('{!r}'.format(ndata[max(0, error.args[2] - 20):min(len(ndata), error.args[3] + 20)]))
	except Exception as e:
		util.logMessage('error decoding {}: {}\n{}'.format(url, e, traceback.format_exc()))

	with open(decodeFailureDumpFile, 'wb') as f:
		f.write(data)
	return None

