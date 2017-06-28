#!/usr/bin/env python2

import sys
import urllib2
import urllib
from bs4 import BeautifulSoup
import traceback
import dao
from threading import Thread


def download(vUrl, out):
    # uses savido to download video
    url = "http://www.savido.net/download?url=" + vUrl

    doc = urllib2.urlopen(url).read()
    soup = BeautifulSoup(doc, "lxml")

    # get the download link
    vid = soup.select("td > a")[0].get("href")

    urllib.urlretrieve(vid, "%s.mp4" % out)

def downloadVids():
    while True:
        try:
            video = dao.getToDownload()
            url = video["url"]
            print "Dowloading " + url
            outfile = "%s/%s" % (sys.argv[1], video["id"])
            download(url, outfile)
            dao.addUrl(video["urlid"], url, 4 if video["status"] == 3 else 5)
            print "--Downloaded " + url
        except Exception, e:
            print >> sys.stderr, "DL " + type(e).__name__ + " " + str(e) + " " + url
            traceback.print_exc()

if __name__ == "__main__":
    threads = []
    downloadThreads = 1
    try:
        downloadThreads = int(sys.argv[2])
    except IndexError:
        pass

    for i in range(0, downloadThreads):
        t = Thread(target=downloadVids)
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

