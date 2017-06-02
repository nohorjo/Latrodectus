#!/usr/bin/env python2

import re
import urllib2
import httplib
from bs4 import BeautifulSoup
import dao
import string
from threading import Thread
import sys
import json


def getDocSoup(url):
    try:
        doc = urllib2.urlopen(url).read()
    except httplib.IncompleteRead, e:
        doc = e.partial
    return BeautifulSoup(doc, "lxml")


def getVidUrl(link, url, nameExtractor, durationExtractor):
    vidurl = link.get("href")
    if vidurl is None:
        vidurl = link["u"]
        name = link["t"]
        durString = link["d"]
        print vidurl
    else:
        print vidurl
        name = str(filter(lambda x: x in set(string.printable), eval(nameExtractor)))
        print "\tname: " + name
        durString = eval(durationExtractor)
    duration = int(re.sub("[^\\d]", "", durString))
    print "\tduration: " + str(duration)
    if duration not in range(10, 25) or dao.isBlackListed(
            name.split(" ")):
        print "\t--Duration/name check failed"
        return None
    if not vidurl.startswith("http"):
        urlRootIndex = url.find("/", 9)
        if urlRootIndex != -1:
            url = url[:urlRootIndex]
        vidurl = url + vidurl

    return str(vidurl)


def getLinks(level, urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck, tagsExtractor):
    rootpage = getDocSoup(url)
    vidlinks = eval(linkExtractor)

    if len(vidlinks) == 0:
        raise Exception("NO VIDEOS FOUND: " + url)

    for link in vidlinks:
        try:
            vidurl = getVidUrl(link, url, nameExtractor, durationExtractor)
            if vidurl is None:
                continue
            if not dao.siteAdded(urlid, vidurl):
                vidpage = getDocSoup(vidurl)
                tags = []
                for tag in vidpage.select(tagsExtractor):
                    tags.append(tag.get_text())
                if not eval(dateCheck) and dao.isBlackListed(tags):
                    print "\t--Date/tag check failed"
                    break
                print str(level) + "\t" + vidurl
                dao.addUrl(urlid, vidurl)
                getLinks(level + 1, urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck,
                         tagsExtractor)
            else:
                print "\t--Duplicate video"
        except Exception, e:
            print >> sys.stderr, type(e).__name__ + " " + str(e) + " " + url


threads = []

for site in dao.getSites():
    print "Starting crawl: " + site[1]
    t = Thread(target=getLinks, args=(0, site[0], site[1], site[2], site[3], site[4], site[5], site[6]))
    t.setDaemon(True)
    t.start()
    threads.append(t)

running = True
while running:
    running = False
    for t in threads:
        if t.isAlive():
            running = True
            break
