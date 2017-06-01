#!/usr/bin/env python2

import sys
import urllib2
import urllib
from bs4 import BeautifulSoup

try:
    vUrl = sys.argv[1]
    outDir = sys.argv[2]
except IndexError:
    print "Error: provide a url and output directory"
    exit(1)

# uses savido to download video
url = "http://www.savido.net/download?url=" + vUrl

doc = urllib2.urlopen(url).read()
soup = BeautifulSoup(doc, "lxml")

# get the download link
vid = soup.select("td > a")[0].get("href")

vUrl = vUrl[:-1]
urllib.urlretrieve(vid, "%s/%s.mp4" % (outDir, vUrl[vUrl.rfind("/"):]))
