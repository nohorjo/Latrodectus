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
import traceback


def getDocSoup(url):
    try:
        doc = urllib2.urlopen(url).read()
    except httplib.IncompleteRead, e:
        doc = e.partial
    return BeautifulSoup(doc, "lxml")


def getVidUrl(link, url, nameExtractor, durationExtractor):
    vidurl = link.get("href")
    error = False
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
        error = True
    if not vidurl.startswith("http"):
        urlRootIndex = url.find("/", 9)
        if urlRootIndex != -1:
            url = url[:urlRootIndex]
        vidurl = url + vidurl
    return {"url": str(vidurl), "error": error}


def dateTagCheck(dateCheck, tagsExtractor, vidurl):
    error = False
    vidpage = getDocSoup(vidurl)
    tags = []
    for tag in vidpage.select(tagsExtractor):
        tags.append(tag.get_text())
    if not eval(dateCheck) and dao.isBlackListed(tags):
        print "\t--Date/tag check failed"
        error = True
    return error


def getLinks(urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck, tagsExtractor):
    rootpage = getDocSoup(url)
    vidlinks = eval(linkExtractor)
    error = False

    if len(vidlinks) == 0:
        raise Exception("NO VIDEOS FOUND: " + url)

    for link in vidlinks:
        try:
            vidtest = getVidUrl(link, url, nameExtractor, durationExtractor)
            vidurl = vidtest["url"]
            error = vidtest["error"]

            if dao.vidStatus(vidurl) < 2:
                if not error: error = dateTagCheck(dateCheck, tagsExtractor, vidurl)
                if not error:
                    print "***\t" + vidurl
                dao.addUrl(urlid, vidurl, 0 if error else 1)
            else:
                print "\t--Duplicate video"
        except Exception, e:
            print >> sys.stderr, type(e).__name__ + " " + str(e) + " " + url

    dao.addUrl(urlid, url, -1)


def startCrawl(urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck, tagsExtractor):
    while True:
        dao.clean()
        url = dao.getResumeUrl(urlid, url)
        print "Starting crawl: " + url
        try:
            getLinks(urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck, tagsExtractor)
        except Exception, e:
            print >> sys.stderr, type(e).__name__ + " " + str(e) + " " + url
            traceback.print_exc()


threads = []
for site in dao.getSites():
#for site in [dao.getSites()[3]]:
    t = Thread(target=startCrawl, args=(site[0], site[1], site[2], site[3], site[4], site[5], site[6]))
    t.setDaemon(True)
    t.start()
    threads.append(t)
#    break

running = True
while running:
    running = False
    for t in threads:
        if t.isAlive():
            running = True
            break
