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


def compoundUrl(root, path):
    if not path.startswith("http"):
        urlRootIndex = root.find("/", 9)
        if urlRootIndex != -1:
            url = root[:urlRootIndex]
        path = url + path
    return path


def getVidUrl(link, url, nameExtractor, durationExtractor):
    try:
        vidurl = link.get("href")
        error = False
        if vidurl is None:
            vidurl = link["u"]
            name = link["t"]
            durString = link["d"]
            vidurl = compoundUrl(url, vidurl)
            print vidurl
        else:
            vidurl = compoundUrl(url, vidurl)
            print vidurl
            name = eval(nameExtractor)
            print "\tname: " + name
            durString = eval(durationExtractor)
        name = str(filter(lambda x: x in set(string.printable), name))
        duration = int(re.sub("[^\\d]", "", durString))
        print "\tduration: " + str(duration)
        if duration not in range(10, 25) or dao.isBlackListed(
                name.split(" ")):
            print "\t--Duration/name check failed"
            error = True

        return {"url": str(vidurl), "error": error}
    except:
        print >> sys.stderr, "GU error " + vidurl
        raise


def dateTagCheck(dateCheck, tagsExtractor, vidurl):
    try:
        error = False
        vidpage = getDocSoup(vidurl)
        tags = []
        for tag in vidpage.select(tagsExtractor):
            tags.append(tag.get_text())
        if not eval(dateCheck) and dao.isBlackListed(tags):
            print "\t--Date/tag check failed"
            error = True
        return error
    except:
        print >> sys.stderr, "DT error " + vidurl
        raise


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
            print >> sys.stderr, "GL " + type(e).__name__ + " " + str(e) + " " + url

    dao.addUrl(urlid, url, -1)


def startCrawl(urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck, tagsExtractor):
    while True:
        dao.clean()
        url = dao.getResumeUrl(urlid, url)
        print "Starting crawl: " + url
        try:
            getLinks(urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck, tagsExtractor)
        except Exception, e:
            print >> sys.stderr, "SC " + type(e).__name__ + " " + str(e) + " " + url
            traceback.print_exc()
        print "Finished crawl: " + url


threads = []
sites = dao.getSites()
for site in sites:
#for site in (sites[2], sites[5],):
    t = Thread(target=startCrawl, args=(site[0], site[1], site[2], site[3], site[4], site[5], site[6]))
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
