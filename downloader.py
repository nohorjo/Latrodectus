#!/usr/bin/env python2

import sys
import urllib2
import urllib
from bs4 import BeautifulSoup


def download(vUrl, out):
    # uses savido to download video
    url = "http://www.savido.net/download?url=" + vUrl

    doc = urllib2.urlopen(url).read()
    soup = BeautifulSoup(doc, "lxml")

    # get the download link
    vid = soup.select("td > a")[0].get("href")

    urllib.urlretrieve(vid, "%s.mp4" % out)


if __name__ == "__main__":
    try:
        download(sys.argv[1], sys.argv[2])
    except IndexError:
        print "Error: provide a url and output filename"
        exit(1)
