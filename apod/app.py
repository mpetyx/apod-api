"""
A micro-service passing back enhanced information from Astronomy
Picture of the Day (APOD).

Adapted from code in https://github.com/nasa/planetary-api
Dec 1, 2015 (written by Dan Hammer)

@author=danhammer
@author=bathomas @email=brian.a.thomas@nasa.gov
@author=jnbetancourt @email=jennifer.n.betancourt@nasa.gov
"""

import json
import logging
from datetime import datetime, date
from datetime import timedelta
from random import sample
import json
import requests
from bs4 import BeautifulSoup
from chalice import Chalice

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARN)

app = Chalice(app_name="apod")
app.debug = True


LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# this should reflect both this service and the backing
# assorted libraries
SERVICE_VERSION = 'v1'
APOD_METHOD_NAME = 'apod'
ALLOWED_APOD_FIELDS = ['concept_tags', 'date', 'hd', 'count', 'start_date', 'end_date']
ALCHEMY_API_KEY = None

try:
    with open('alchemy_api.key', 'r') as f:
        ALCHEMY_API_KEY = f.read()
except FileNotFoundError:
    LOG.info('WARNING: NO alchemy_api.key found, concept_tagging is NOT supported')

"""
Split off some library functions for easier testing and code management.

Created on Mar 24, 2017

@author=bathomas @email=brian.a.thomas@nasa.gov
"""

# location of backing APOD service
BASE = 'https://apod.nasa.gov/apod/'


def _get_apod_chars(dt):
    media_type = 'image'
    date_str = dt.strftime('%y%m%d')
    apod_url = '%sap%s.html' % (BASE, date_str)
    LOG.debug('OPENING URL:' + apod_url)
    soup = BeautifulSoup(requests.get(apod_url).text, 'html.parser')
    LOG.debug('getting the data url')
    hd_data = None
    if soup.img:
        # it is an image, so get both the low- and high-resolution data
        data = BASE + soup.img['src']
        hd_data = data

        LOG.debug('getting the link for hd_data')
        for link in soup.find_all('a', href=True):
            if link['href'] and link['href'].startswith('image'):
                hd_data = BASE + link['href']
                break
    else:
        # its a video
        media_type = 'video'
        data = soup.iframe['src']

    props = {}

    props['explanation'] = _explanation(soup)
    props['title'] = _title(soup)
    copyright_text = _copyright(soup)
    if copyright_text:
        props['copyright'] = copyright_text
    props['media_type'] = media_type
    props['url'] = data
    props['date'] = dt.isoformat()

    if hd_data:
        props['hdurl'] = hd_data

    return props


def _title(soup):
    """
    Accepts a BeautifulSoup object for the APOD HTML page and returns the
    APOD image title.  Highly idiosyncratic with adaptations for different
    HTML structures that appear over time.
    """
    LOG.debug('getting the title')
    try:
        # Handler for later APOD entries
        center_selection = soup.find_all('center')[1]
        bold_selection = center_selection.find_all('b')[0]
        return bold_selection.text.strip(' ')
    except Exception:
        # Handler for early APOD entries
        text = soup.title.text.split(' - ')[-1]
        return text.strip()


def _copyright(soup):
    """
    Accepts a BeautifulSoup object for the APOD HTML page and returns the
    APOD image copyright.  Highly idiosyncratic with adaptations for different
    HTML structures that appear over time.
    """
    LOG.debug('getting the copyright')
    try:
        # Handler for later APOD entries
        # There's no uniform handling of copyright (sigh). Well, we just have to
        # try every stinking text block we find...

        copyright_text = None
        use_next = False
        for element in soup.findAll('a', text=True):
            # LOG.debug("TEXT: "+element.text)

            if use_next:
                copyright_text = element.text.strip(' ')
                break

            if 'Copyright' in element.text:
                LOG.debug('Found Copyright text:' + str(element.text))
                use_next = True

        if not copyright_text:

            for element in soup.findAll(['b', 'a'], text=True):
                # search text for explicit match
                if 'Copyright' in element.text:
                    LOG.debug('Found Copyright text:' + str(element.text))
                    # pull the copyright from the link text which follows
                    sibling = element.next_sibling
                    stuff = ""
                    while sibling:
                        try:
                            stuff = stuff + sibling.text
                        except Exception:
                            pass
                        sibling = sibling.next_sibling

                    if stuff:
                        copyright_text = stuff.strip(' ')

        return copyright_text

    except Exception as ex:
        LOG.error(str(ex))
        raise ValueError('Unsupported schema for given date.')


def _explanation(soup):
    """
    Accepts a BeautifulSoup object for the APOD HTML page and returns the
    APOD image explanation.  Highly idiosyncratic.
    """
    # Handler for later APOD entries
    LOG.debug('getting the explanation')
    s = soup.find_all('p')[2].text
    s = s.replace('\n', ' ')
    s = s.replace('  ', ' ')
    s = s.strip(' ').strip('Explanation: ')
    s = s.split(' Tomorrow\'s picture')[0]
    s = s.strip(' ')
    if s == '':
        # Handler for earlier APOD entries
        texts = [x.strip() for x in soup.text.split('\n')]
        try:
            begin_idx = texts.index('Explanation:') + 1
        except ValueError as e:
            # Rare case where "Explanation:" is not on its own line
            explanation_line = [x for x in texts if "Explanation:" in x]
            if len(explanation_line) == 1:
                begin_idx = texts.index(explanation_line[0])
                texts[begin_idx] = texts[begin_idx][12:].strip()
            else:
                raise e

        idx = texts[begin_idx:].index('')
        s = ' '.join(texts[begin_idx:begin_idx + idx])
    return s


def parse_apod(dt, use_default_today_date=False):
    """
    Accepts a date in '%Y-%m-%d' format. Returns the URL of the APOD image
    of that day, noting that
    """

    LOG.debug('apod chars called date:' + str(dt))

    try:
        return _get_apod_chars(dt)

    except Exception as ex:

        # handle edge case where the service local time
        # miss-matches with 'todays date' of the underlying APOD
        # service (can happen because they are deployed in different
        # timezones). Use the fallback of prior day's date

        if use_default_today_date:
            # try to get the day before
            dt = dt - timedelta(days=1)
            return _get_apod_chars(dt)
        else:
            # pass exception up the call stack
            LOG.error(str(ex))
            raise Exception(ex)


def get_concepts(request, text, apikey):
    """
    Returns the concepts associated with the text, interleaved with integer
    keys indicating the index.
    """
    cbase = 'http://access.alchemyapi.com/calls/text/TextGetRankedConcepts'

    params = dict(
        outputMode='json',
        apikey=apikey,
        text=text
    )

    try:

        LOG.debug('Getting response')
        response = json.loads(request.get(cbase, fields=params))
        clist = [concept['text'] for concept in response['concepts']]
        return {k: v for k, v in zip(range(len(clist)), clist)}

    except Exception as ex:
        raise ValueError(ex)


def _abort(code, msg, usage=True):
    if usage:
        msg += " " + "'"

    response = json.dumps(service_version=SERVICE_VERSION, msg=msg, code=code)
    response.status_code = code
    LOG.debug(str(response))

    return response


def _validate(data):
    LOG.debug('_validate(data) called')
    for key in data:
        if key not in ALLOWED_APOD_FIELDS:
            return False
    return True


def _validate_date(dt):
    LOG.debug('_validate_date(dt) called')
    today = datetime.today().date()
    begin = datetime(1995, 6, 16).date()  # first APOD image date

    # validate input
    if (dt > today) or (dt < begin):
        today_str = today.strftime('%b %d, %Y')
        begin_str = begin.strftime('%b %d, %Y')

        raise ValueError('Date must be between %s and %s.' % (begin_str, today_str))


def _apod_handler(dt, use_concept_tags=False, use_default_today_date=False):
    """
    Accepts a parameter dictionary. Returns the response object to be
    served through the API.
    """
    try:
        page_props = parse_apod(dt, use_default_today_date)
        LOG.debug('managed to get apod page characteristics')

        if use_concept_tags:
            if ALCHEMY_API_KEY is None:
                page_props['concepts'] = 'concept_tags functionality turned off in current service'
            else:
                page_props['concepts'] = get_concepts(request, page_props['explanation'], ALCHEMY_API_KEY)

        return page_props

    except Exception as e:

        LOG.error('Internal Service Error :' + str(type(e)) + ' msg:' + str(e))
        # return code 500 here
        return _abort(500, 'Internal Service Error', usage=False)


def _get_json_for_date(input_date, use_concept_tags):
    """
    This returns the JSON data for a specific date, which must be a string of the form YYYY-MM-DD. If date is None,
    then it defaults to the current date.
    :param input_date:
    :param use_concept_tags:
    :return:
    """

    # get the date param
    use_default_today_date = False
    if not input_date:
        # fall back to using today's date IF they didn't specify a date
        input_date = datetime.strftime(datetime.today(), '%Y-%m-%d')
        use_default_today_date = True

    # validate input date
    dt = datetime.strptime(input_date, '%Y-%m-%d').date()
    _validate_date(dt)

    # get data
    data = _apod_handler(dt, use_concept_tags, use_default_today_date)
    data['service_version'] = SERVICE_VERSION

    # return info as JSON
    return json.dumps(data)


def _get_json_for_random_dates(count, use_concept_tags):
    """
    This returns the JSON data for a set of randomly chosen dates. The number of dates is specified by the count
    parameter
    :param count:
    :param use_concept_tags:
    :return:
    """

    if count > 100 or count <= 0:
        raise ValueError('Count must be positive and cannot exceed 100')

    begin_ordinal = datetime(1995, 6, 16).toordinal()
    today_ordinal = datetime.today().toordinal()

    date_range = range(begin_ordinal, today_ordinal + 1)
    random_date_ordinals = sample(date_range, count)

    all_data = []
    for date_ordinal in random_date_ordinals:
        dt = date.fromordinal(date_ordinal)
        data = _apod_handler(dt, use_concept_tags, date_ordinal == today_ordinal)
        data['service_version'] = SERVICE_VERSION
        all_data.append(data)

    return json.dumps(all_data)


def _get_json_for_date_range(start_date, end_date, use_concept_tags):
    """
    This returns the JSON data for a range of dates, specified by start_date and end_date, which must be strings of the
    form YYYY-MM-DD. If end_date is None then it defaults to the current date.
    :param start_date:
    :param end_date:
    :param use_concept_tags:
    :return:
    """
    # validate input date
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    _validate_date(start_dt)

    # get the date param
    if not end_date:
        # fall back to using today's date IF they didn't specify a date
        end_date = datetime.strftime(datetime.today(), '%Y-%m-%d')

    # validate input date
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    _validate_date(end_dt)

    start_ordinal = start_dt.toordinal()
    end_ordinal = end_dt.toordinal()
    today_ordinal = datetime.today().date().toordinal()

    if start_ordinal > end_ordinal:
        raise ValueError('start_date cannot be after end_date')

    all_data = []

    while start_ordinal <= end_ordinal:
        # get data
        dt = date.fromordinal(start_ordinal)
        data = _apod_handler(dt, use_concept_tags, start_ordinal == today_ordinal)
        data['service_version'] = SERVICE_VERSION

        if data['date'] == dt.isoformat():
            # Handles edge case where server is a day ahead of NASA APOD service
            all_data.append(data)

        start_ordinal += 1

    # return info as JSON
    return json.dumps(all_data)


#
# Endpoints
#



@app.route('/' + SERVICE_VERSION + '/' + APOD_METHOD_NAME , methods=['GET'], cors=True)
def apod():
    LOG.info('apod path called')
    try:
        request = app.current_request

        # application/json GET method

        args = request.query_params
        if not _validate(args):
            return _abort(400, 'Bad Request: incorrect field passed.')

        #
        input_date = args.get('date')
        count = args.get('count')
        start_date = args.get('start_date')
        end_date = args.get('end_date')
        use_concept_tags = args.get('concept_tags', False)

        if not count and not start_date and not end_date:
            return _get_json_for_date(input_date, use_concept_tags)

        elif not input_date and not start_date and not end_date and count:
            return _get_json_for_random_dates(int(count), use_concept_tags)

        elif not count and not input_date and start_date:
            return _get_json_for_date_range(start_date, end_date, use_concept_tags)

        else:
            return _abort(400, 'Bad Request: invalid field combination passed.')

    except ValueError as ve:
        return _abort(400, str(ve), False)

    except Exception as ex:

        etype = type(ex)
        if etype == ValueError or 'BadRequest' in str(etype):
            return _abort(400, str(ex) + ".")
        else:
            LOG.error('Service Exception. Msg: ' + str(type(ex)))
            return _abort(500, 'Internal Service Error', usage=False)
