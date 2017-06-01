#!/usr/bin/env python2

import re
import urllib2
import httplib
from bs4 import BeautifulSoup
import dao
import traceback
import string
from threading import Thread


def getLinks(level, urlid, url, linkSelector, nameExtractor, durationExtractor, dateCheck, tagsExtractor):
    try:
        vidlinks = BeautifulSoup(urllib2.urlopen(url).read(), "lxml").select(linkSelector)
        for link in vidlinks:
            vidurl = link.get("href")
            name = str(filter(lambda x: x in set(string.printable), eval(nameExtractor)))
            if int(re.sub("[^\\d]", "", eval(durationExtractor))) in range(10, 25) or dao.isBlackListed(
                    name.split(" ")):
                continue
            if not vidurl.startswith("http"):
                vidurl = url + link.get("href")
            try:
                vidpage = BeautifulSoup(urllib2.urlopen(vidurl).read(), "lxml")
            except httplib.IncompleteRead, e:
                vidpage = BeautifulSoup(e.partial, "lxml")
            tags = []
            for tag in vidpage.select(tagsExtractor):
                tags.append(tag.get_text())
            if not eval(dateCheck) and dao.isBlackListed(tags):
                continue
            if not dao.addUrl(urlid, vidurl):
                print "Duplicate video"
            print str(level) + " " + vidurl
            getLinks(level + 1, urlid, url, linkSelector, nameExtractor, durationExtractor, dateCheck, tagsExtractor)
    except Exception, e:
        print "Error " + str(e)
        traceback.print_exc()
        print "params:"
        print (level, urlid, url, linkSelector, nameExtractor, durationExtractor, dateCheck, tagsExtractor)


for site in dao.getSites():
    t = Thread(target=getLinks, args=(0, site[0], site[1], site[2], site[3], site[4], site[5], site[6]))
    t.start()
