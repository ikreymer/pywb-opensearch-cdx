from pywb.cdx.cdxserver import CDXServer
from pywb.utils.timeutils import timestamp_now
from pywb.utils.timeutils import pad_timestamp
from pywb.cdx.cdxobject import CDXObject
from pywb.cdx.cdxops import cdx_sort_closest
from pywb.utils.canonicalize import canonicalize

from pywb.utils.wbexception import WbException, NotFoundException

from io import BytesIO

try:
    from lxml import etree
    print('Using LXML')
except:
    import xml.etree.ElementTree as etree
    print('Not using LXML')

from urllib import quote_plus

import requests


#=============================================================================
EARLIEST_DATE = '19960101000000'
LATEST_DATE = '29991231235959'


#=============================================================================
class OpenSearchCDXServer(CDXServer):
    CLOSEST_QUERY_FIXED = '&hitsPerPage=10000&start=0&dedupField=site&hitsPerDup=10000&hitsPerSite=10000&waybackQuery=true'

    def __init__(self, paths, **kwargs):
        self.opensearch_query = paths

    def load_cdx(self, **params):
        closest = params.get('closest')

        self.check_url(params)

        if closest:
            query = self._get_closest_query(params)
        else:
            query = self._get_timemap_query(params)

        query = quote_plus(query) + self.CLOSEST_QUERY_FIXED
        full_url = self.opensearch_query + '?query=' + query
        print('QUERY', full_url)

        output = params.get('output', 'text')
        url = params.get('url')
        urlkey = canonicalize(url)

        try:
            response = requests.get(full_url, stream=True)
            buff = response.raw.read()
            response.raw.close()
        except Exception as e:
            import traceback
            traceback.print_exc(e)
            raise WbException(e)

        results = etree.fromstring(buff)

        items = results.find('channel').findall('item')

        cdx_list = [self.convert_to_cdx(item, urlkey, url) for item in items]

        if not cdx_list:
            raise NotFoundException('url {0} not found'.format(url))

        if closest:
            cdx_list = cdx_sort_closest(closest, cdx_list, limit=10000)
        else:
            cdx_list = cdx_sort_closest(EARLIEST_DATE, cdx_list, limit=10000)

        if output == 'text':
            cdx_list = [str(cdx) + '\n' for cdx in cdx_list]
        elif output == 'json':
            fields = params.get('fl', '').split(',')
            cdx_list = [cdx.to_json(fields) for cdx in cdx_list]

        return iter(cdx_list)


    def convert_to_cdx(self, item, urlkey, url):
        cdx = CDXObject()
        cdx['urlkey'] = canonicalize(url)
        cdx['timestamp'] = gettext(item, 'tstamp')[:14]
        cdx['url'] = url
        cdx['mime'] = gettext(item, 'primaryType') + '/' + gettext(item, 'subType')
        cdx['status'] = '-'
        cdx['digest'] = gettext(item, 'digest')
        #cdx['length'] = gettext(item, 'contentLength')
        cdx['length'] = '-'
        cdx['offset'] = gettext(item, 'arcoffset')
        cdx['filename'] = gettext(item, 'arcname') + '.arc.gz'
        return cdx

    def _get_closest_query(self, params):
        closest = params.get('closest')
        closest = pad_timestamp(closest, EARLIEST_DATE)
        query = 'closestdate:{0} exacturl:{1}'.format(closest,
                                                      params.get('url'))
        return query

    def _get_timemap_query(self, params):
        from_ts = params.get('from')
        if from_ts:
            from_ts = pad_timestamp(from_ts, EARLIEST_DATE)
        else:
            from_ts = EARLIEST_DATE

        to_ts = params.get('to')
        if not to_ts:
            to_ts = timestamp_now()
        else:
            to_ts = pad_timestamp(to_ts, LATEST_DATE)

        query = 'exacturlexpand:{0} date:{1}-{2}'.format(params.get('url'),
                                                         from_ts, to_ts)

        return query

    def check_url(self, params):
        url = params.get('url')
        if not url:
            raise WbException('url= param is missing!')

        if url.startswith(('http://', 'https://')):
            return
        elif url.startswith('//'):
            url = 'http:' + url
        else:
            url = 'http://' + url

        params['url'] = url


def gettext(item, name):
    elem = item.find('{http://arquivo.pt/opensearchrss/1.0/}' + name)
    if elem is not None:
        return elem.text
    else:
        return '-'
