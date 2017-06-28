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
import js2py
import downloader


def getDocSoup(url, cookie=""):
    try:
        opener = urllib2.build_opener()
        opener.addheaders.append(('Cookie', cookie))
        doc = opener.open(url).read()
    except httplib.IncompleteRead, e:
        doc = e.partial
    return BeautifulSoup(doc, "lxml")


def compoundUrl(root, path):
    if not path.startswith("http"):
        urlRootIndex = root.find("/", 9)
        if urlRootIndex != -1:
            root = root[:urlRootIndex]
        path = root + path
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


def dateTagCheck(dateCheck, tagsExtractor, vidpage):
    try:
        error = False
        tags = []
        for tag in vidpage.select(tagsExtractor):
            tags.append(tag.get_text())
        if not eval(dateCheck) and dao.isBlackListed(tags):
            print "\t--Date/tag check failed"
            error = True
        return error
    except:
        print >> sys.stderr, "DT error"
        raise


def getLinks(urlid, url, linkExtractor, nameExtractor, durationExtractor, dateCheck, tagsExtractor):
    try:
        rootpage = getDocSoup(url)
        vidlinks = eval(linkExtractor)

        if len(vidlinks) == 0:
            try:
                cookie = js2py.eval_js(re.sub(".*<!--", "", re.sub("//-->.*", "",
                                                                   rootpage.get_text().replace("document.cookie=",
                                                                                               "return ").replace(
                                                                       "document.location.reload(true);", "").replace(
                                                                       "Loading ...", ""))) + " go()")
                rootpage = getDocSoup(url, cookie)
                vidlinks = eval(linkExtractor)
            except:
                pass
            if len(vidlinks) == 0:
                print >> sys.stderr, "NO VIDEOS FOUND: " + url
                return
    except (urllib2.HTTPError, urllib2.URLError), e:
        print >> sys.stderr, "GL " + type(e).__name__ + " " + str(e) + " " + url
        return

    for link in vidlinks:
        try:
            vidtest = getVidUrl(link, url, nameExtractor, durationExtractor)
            vidurl = vidtest["url"]
            error = vidtest["error"]

            if dao.vidStatus(vidurl) == -1:
                dao.addUrl(urlid, vidurl, 0 if error else 1)
            else:
                print "\t--Duplicate video"
        except Exception, e:
            print >> sys.stderr, "GL " + type(e).__name__ + " " + str(e) + " " + url

        status = dao.vidStatus(url)
        if status == 1:
            if not dateTagCheck(dateCheck, tagsExtractor, rootpage):
                print "***\t" + vidurl
            else:
                status = 0;
    dao.addUrl(urlid, url, status + 2)


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

if __name__ == "__main__":
    threads = []
    sites = dao.getSites()
    
    threadMultiplyier = 1
    try:
        threadMultiplyier = int(sys.argv[1])
    except IndexError:
        pass
    
    for i in range(0, threadMultiplyier):
		for site in sites:
			# for site in (sites[4],):
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
