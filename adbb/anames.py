#!/usr/bin/env python
#
# This file is part of adbb.
#
# adbb is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# adbb is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with adbb.  If not, see <http://www.gnu.org/licenses/>.

import datetime
import difflib
import gzip
import os
import tempfile
import time
import xml.etree.ElementTree as etree

import urllib
import urllib.error
import urllib.request

import adbb.animeobjs
from adbb.errors import AniDBError, AniDBFileError

_animetitles_useragent="adbb"
_animetitles_url="https://anidb.net/api/anime-titles.xml.gz"
_anime_list_url="https://github.com/Anime-Lists/anime-lists/raw/master/anime-list.xml"
iso_639_file=os.path.join(os.path.dirname(os.path.abspath(__file__)), "ISO-639-2_utf-8.txt")
_update_interval = datetime.timedelta(hours=36)

titles = None
_anime_titles = None
_title_to_aids = None
_all_title_pairs = None
anilist = None
absolute_order_set = None
languages = None

def update_xml(url):
    file_name = url.split('/')[-1]
    ext = url.split('.')[-1]
    if os.name == 'posix':
        cache_file = os.path.join('/var/tmp', file_name)
    else:
        cache_file = os.path.join(tempfile.gettempdir(), file_name)

    tmp_dir = os.path.dirname(cache_file)
    if not os.access(tmp_dir, os.W_OK):
        raise AniDBError("Cant get writeable temp path: %s" % tmp_dir)

    old_file_exists = os.path.isfile(cache_file)
    if old_file_exists:
        stat = os.stat(cache_file)
        file_moddate = datetime.datetime.fromtimestamp(stat.st_mtime)
        if file_moddate > (datetime.datetime.now() - _update_interval):
            return cache_file

    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S.%f")
    tmp_file = os.path.join(os.path.dirname(cache_file), f".adbb_cache{now}.{ext}")

    try:
        with open(tmp_file, "wb") as f:
            req = urllib.request.Request(
                url,
                data=None,
                headers={
                    'User-Agent': _animetitles_useragent
                }
            )
            res = urllib.request.urlopen(req, timeout=30)
            adbb.log.info(f'Fetching cache file from {url}')
            f.write(res.read())
    except (IOError, urllib.error.URLError) as err:
        adbb.log.error(f"Failed to fetch {url}: {err}")
        adbb.log.info("You may be temporary ip-banned from anidb, banns will be automatically lifted after 24 hours!")
        os.remove(tmp_file)
        if old_file_exists:
            return cache_file
        return None
    
    if not _verify_xml_file(tmp_file):
        adbb.log.error("Failed to verify xml file: {}".format(tmp_file))
        return None

    os.rename(tmp_file, cache_file)
    return cache_file

def update_anilist():
    # These are the global variables we want to update
    # reset them here.
    global anilist, absolute_order_set
    anilist = {}
    absolute_order_set = set()

    xml_file = update_xml(_anime_list_url)
    if not xml_file and not anilist:
        adbb.log.critical("Missing, and unable to fetch, list of anime mappings")
        raise AniDBFileError("Missing, and unable to fetch, list of anime mappings")
    xml = _read_anidb_xml(xml_file)
    absolute_order = {}

    # Iterate every anime entry in XML; save attributes in the anilist dict.
    for anime in xml.iter("anime"):
        aid=anime.attrib['anidbid']
        a_attrs = anime.attrib
        del a_attrs['anidbid']

        # keep track if this anime has the absolute order flag set and if it
        # has any other non-special seasons explicitly specified
        has_absolute_order = False
        has_season_mapping = False
        if 'defaulttvdbseason' in a_attrs and 'tvdbid' in a_attrs and a_attrs['defaulttvdbseason'] == "a":
            has_absolute_order = True

        if has_absolute_order and not a_attrs['tvdbid'] in absolute_order:
            absolute_order[a_attrs['tvdbid']] = set()

        anilist[aid] = a_attrs
        mappings=anime.find('mapping-list')
        if mappings:
            anilist[aid]['map'] = []
            for m in mappings.iter("mapping"):
                attrs = m.attrib

                # Check for non-special seasons for series with
                # defaulttvdbseason set to absolute
                if has_absolute_order:
                    if all([x in attrs for x in ['tvdbseason', 'start']]) and int(attrs['tvdbseason']) > 0:
                        absolute_order[a_attrs['tvdbid']].add(int(attrs['tvdbseason']))
                        has_season_mapping = True

                if m.text:
                    attrs['epmap'] = {}
                    episodes = m.text.strip(';').split(';')
                    for e in episodes:
                        (a, t) = e.split('-')
                        attrs['epmap'][a] = t

                    # If multiple anidb episodes are mapped to the same tvdb
                    # episode we need to figure out partnumbers; this is
                    # unfortunately broken for movies because of how anidb adds
                    # parts with episode numbers. When scraping movies the part
                    # should probably be ignored.
                    anidb_eps = sorted(attrs['epmap'].keys(), key=lambda x: int(x))
                    newmap = {}
                    for anidb_ep in anidb_eps:
                        my_epno = attrs['epmap'][anidb_ep]
                        others = [ x for x in anidb_eps if attrs['epmap'][x] == my_epno]
                        if len(others) == 1:
                            newmap[anidb_ep] = my_epno
                        else:
                            part = others.index(anidb_ep)+1
                            newmap[anidb_ep] = (my_epno, part)
                    attrs['epmap'] = newmap

                anilist[aid]['map'].append(attrs)

        # no non-special season specified; we must treat this tvdb entry as
        # absolute order.
        if has_absolute_order and not has_season_mapping:
            absolute_order_set.add(a_attrs['tvdbid'])
            del absolute_order[a_attrs['tvdbid']]
        name=anime.find('name')
        anilist[aid]['name']=name.text

    # Now that we have gone through all entries, check if there are season
    # mappings for all seasons for the default-absolute-order series. If there
    # is, we don't need to treat is as absolute ordered.
    for tvdbid,seasons in absolute_order.items():
        season_list = sorted(seasons)
        if len(season_list) != season_list[-1]:
            absolute_order_set.add(tvdbid)


def update_animetitles():
    global titles
    xml_file = update_xml(_animetitles_url)
    if not xml_file and not titles:
        adbb.log.critical("Missing, and unable to fetch, list of anime titles")
        raise AniDBFileError("Missing, and unable to fetch, list of anime titles")
    titles = _read_anidb_xml(xml_file)
    _build_title_index()


def _verify_xml_file(path):
    if not os.path.isfile(path):
        return False
    
    try:
        tmp_xml = _read_anidb_xml(path)
    except Exception as e:
        adbb.log.error("Exception when reading xml file: {}".format(e))
        return False

    if len(tmp_xml.findall('anime')) < 8000:
        return False
    
    return True
        

def _read_anidb_xml(filePath):
    return _read_xml_into_etree(filePath)


def _read_xml_into_etree(filePath):
        if not filePath:
            return None
        
        if filePath.split('.')[-1] == 'gz':
            with gzip.open(filePath, "rb") as f:
                data = f.read()
        else:
            with open(filePath, 'rb') as f:
                data = f.read()

        xmlASetree = etree.fromstring(data)
        return xmlASetree


def _read_language_file():
    global languages
    languages = {}
    with open(iso_639_file, "r") as f:
        for line in f:
            three, tree2, two, eng, fre = line.strip().split('|')
            if two:
                languages[two] = three


def get_lang_code(short):
    if not languages:
        _read_language_file()

    if short in languages:
        return languages[short]
    return None
    

def _build_title_index():
    """Pre-compute lookup structures from the titles XML.

    Called once after XML load. Builds:
    - _anime_titles: {aid: [AnimeTitle, ...]} for O(1) aid lookup
    - _title_to_aids: {lowercase_title: set(aid)} for O(1) exact match
    - _all_title_pairs: [(aid, title_text), ...] for fuzzy fallback
    """
    global _anime_titles, _title_to_aids, _all_title_pairs
    _anime_titles = {}
    _title_to_aids = {}
    _all_title_pairs = []

    for anime in titles.findall('anime'):
        aid = int(anime.get('aid'))
        anime_title_objs = [
            adbb.animeobjs.AnimeTitle(
                x.get('type'),
                get_lang_code(x.get(
                    '{http://www.w3.org/XML/1998/namespace}lang')),
                x.text) for x in anime.findall('title')]
        _anime_titles[aid] = anime_title_objs

        for title_obj in anime_title_objs:
            lower = title_obj.title.lower()
            if lower not in _title_to_aids:
                _title_to_aids[lower] = set()
            _title_to_aids[lower].add(aid)
            _all_title_pairs.append((aid, title_obj.title))


def get_titles(name=None, aid=None, max_results=10, score_for_match=0.8):
    global titles

    if _anime_titles is None:
        update_animetitles()
    if _anime_titles is None:
        raise AniDBFileError('Could not get valid title cache file.')

    # Aid lookup — O(1)
    if aid and not name:
        if aid in _anime_titles:
            return [(aid, _anime_titles[aid], 1.0, None)]
        return []

    # Name lookup
    if name:
        name = name.replace('\u2044', '/')
        name_lower = name.lower()
        res = []
        seen_aids = set()

        # Phase 1: exact title match — O(1)
        if name_lower in _title_to_aids:
            for matched_aid in _title_to_aids[name_lower]:
                seen_aids.add(matched_aid)
                res.append((
                    matched_aid, _anime_titles[matched_aid],
                    1.0, name))

        if len(res) >= max_results:
            res.sort(key=lambda x: x[2], reverse=True)
            return res[:max_results]

        # Phase 2: substring + fuzzy on pre-computed flat list
        aid_scores = {}
        for pair_aid, pair_title in _all_title_pairs:
            if pair_aid in seen_aids:
                continue
            is_substring = name_lower in pair_title.lower()
            title_score = difflib.SequenceMatcher(
                a=name, b=pair_title).ratio()
            if pair_aid not in aid_scores \
                    or title_score > aid_scores[pair_aid][0]:
                aid_scores[pair_aid] = (
                    title_score, pair_title, is_substring)

        for scored_aid, (score, best_title, is_sub) in aid_scores.items():
            if score > score_for_match or is_sub:
                res.append((
                    scored_aid, _anime_titles[scored_aid],
                    score, best_title))

        res.sort(key=lambda x: x[2], reverse=True)
        return res[:max_results]

    return []


def anilist_maps(aid):
    global anilist
    if not anilist:
        update_anilist()
    if str(aid) in anilist:
        return anilist[str(aid)]
    return {}

def get_tvdbid(aid):
    maps = anilist_maps(aid)
    if 'tvdbid' in maps:
        try:
            int(maps['tvdbid'])
        except ValueError:
            return None
        return maps['tvdbid']
    return None

def get_tmdbid(aid):
    maps = anilist_maps(aid)
    if 'tmdbid' in maps and maps['tmdbid'] not in ['', 'unknown']:
        if ',' in maps['tmdbid']:
            return maps['tmdbid'].split(',')
        return maps['tmdbid']
    return None

def get_imdbid(aid):
    maps = anilist_maps(aid)
    if 'imdbid' in maps and maps['imdbid'] not in ['', 'unknown']:
        if ',' in maps['imdbid']:
            return maps['imdbid'].split(',')
        return maps['imdbid']
    return None

def tvdbid_has_absolute_order(tvdbid):
    global absolute_order_set
    return tvdbid in absolute_order_set

def get_tvdb_episode(aid, epno):
    maps = anilist_maps(aid)
    if not 'tvdbid' in maps:
        return (None, None)

    if 'defaulttvdbseason' in maps:
        tvdb_season = maps['defaulttvdbseason']
    else:
        tvdb_season = None
    anidb_season = "1"
    anidb_special_offset = 0
    if str(epno).upper().startswith('S'):
        anidb_season = "0"
    elif str(epno).upper().startswith('T'):
        anidb_season = "0"
        anidb_special_offset = 200
    elif str(epno).upper().startswith('O'):
        anidb_season = "0"
        anidb_special_offset = 400

    try:
        int_epno = int(str(epno).upper().strip('STO')) + anidb_special_offset
    except ValueError:
        # Only specials of type Special, Trailer or Other are supported by
        # anime-lists
        return (None, None)

    str_epno = str(int_epno)

    if 'map' in maps:
        for m in maps['map']:
            if m['anidbseason'] != anidb_season:
                continue
            if 'epmap' in m:
                if str_epno in m['epmap']:
                    # Exact match for episode
                    tvdb_epno = m['epmap'][str_epno]
                    if tvdb_epno == "0" or isinstance(tvdb_epno, tuple) and tvdb_epno[0] == "0":
                        tvdb_season = None
                        continue
                    if 'tvdbseason' in m:
                        tvdb_season = m['tvdbseason']
                    return (tvdb_season, tvdb_epno)
            if tvdbid_has_absolute_order(maps['tvdbid']) and 'tvdbseason' in m and m['tvdbseason'] != "0":
                # Do not mix absolute and seasoned order...
                continue
            if not all([ x in m for x in ['start', 'end']]):
                continue
            if 'start' in m and int_epno < int(m['start']):
                continue
            if 'end' in m and int_epno > int(m['end']):
                continue
            if 'tvdbseason' in m:
                tvdb_season = m['tvdbseason']
            if 'offset' in m:
                ret_epno = int(m['offset']) + int_epno
                if ret_epno < 1:
                    return (None, None)
                return (tvdb_season, str(ret_epno))
    if not tvdb_season:
        # No season specified or episode mapped to 0
        return (None, None)
    if anidb_season == "0":
        # special, but not explicitly mapped in anime-list
        return ("s", str_epno)

    if 'episodeoffset' in maps:
        ret_epno = int(maps['episodeoffset']) + int_epno
        if ret_epno < 1:
            return (None, None)
        return (tvdb_season, str(int(maps['episodeoffset']) + int_epno))
    return (tvdb_season, str_epno)

