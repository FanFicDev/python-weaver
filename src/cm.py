from typing import TYPE_CHECKING
if TYPE_CHECKING:
	import requests

def getDefaultCookies() -> 'requests.cookies.RequestsCookieJar':
	import requests
	cookies = requests.cookies.RequestsCookieJar()

	# pretend we're an adult for fictionalley
	cookies.set('fauser', 'wizard',
			domain='www.fictionalley.org', path='/authors')

	# fake login cookie for qq
	cookies.set('xf_user', 'xf_user',
			domain='forum.questionablequesting.com', path='/')
	cookies.set('xf_session', 'xf_session',
			domain='forum.questionablequesting.com', path='/')

	# fake adult acceptance on livejournal
	cookies.set('adult_explicit', '1', domain='.livejournal.com', path='/')

	# accept ao3s tos
	cookies.set('accepted_tos', '20180523',
			domain='archiveofourown.org', path='/')

	return cookies

