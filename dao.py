#!/usr/bin/env python2

import sqlite3

dbname = "videos.db"


def init():
    conn = sqlite3.connect(dbname)

    try:
        conn.execute('''
            CREATE TABLE BLACKLIST (
                TERM TEXT PRIMARY KEY NOT NULL
            );
            ''')
        conn.execute('''
            CREATE TABLE SITES (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                SITE TEXT NOT NULL,
                LINK TEXT NOT NULL,
                NAME TEXT NOT NULL,
                DURATION TEXT NOT NULL,
                DATE TEXT NOT NULL,
                TAGS TEXT NOT NULL
            );
            ''')
        conn.execute('''
            CREATE TABLE VIDEOS (
                ID INTEGER,
                URL TEXT PRIMARY KEY NOT NULL,
                SITEID INTEGER NOT NULL REFERENCES SITES(ID),
                CHECKED TINYINT NOT NULL DEFAULT 0
            );''')
    except sqlite3.OperationalError:
        pass
    conn.close()


def addUrl(siteid, url):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO VIDEOS (ID,SITEID, URL) VALUES ((SELECT IFNULL(MAX(ID), 0) + 1 FROM VIDEOS),:siteid,:url);",
        {"siteid": siteid, "url": url})
    conn.commit()
    conn.close()


def siteAdded(siteid, url):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM VIDEOS WHERE SITEID=:siteid AND URL=:url;", {"siteid": siteid, "url": url})
    result = False
    for row in cur:
        if int(row[0]) > 0:
            result = True
    conn.close()
    return result


def setChecked(url):
    conn = sqlite3.connect(dbname)
    conn.execute("UPDATE VIDEOS SET CHECKED = 1 WHERE URL = :url;", {"url": url})
    conn.close()


def getUnchecked():
    urls = []
    conn = sqlite3.connect(dbname)
    cur = conn.execute("SELECT URL FROM VIDEOS WHERE CHECKED = 0;")
    for row in cur:
        urls.append(str(row[0]))
    conn.close()
    return urls


def isBlackListed(tags):
    tags = [str(x).strip() for x in tags]
    conn = sqlite3.connect(dbname)
    sql = "SELECT COUNT(*) FROM BLACKLIST WHERE TERM IN ("
    i = 0;
    t = "{"
    for tag in tags:
        tag = tag.replace("'", "\\'")
        sql += ":tag" + str(i) + ","
        t += "'tag" + str(i) + "':'''" + tag.lower() + "''',"
        i += 1
    sql = sql[:-1] + ");"
    t = t[:-1] + "}"
    cur = conn.execute(sql, eval(t))
    result = False
    for row in cur:
        if int(row[0]) > 0:
            result = True
    conn.close()
    return result


def getSites():
    sites = []
    conn = sqlite3.connect(dbname)
    cur = conn.execute("SELECT ID, SITE, LINK, NAME, DURATION, DATE, TAGS FROM SITES;")
    for row in cur:
        site = []
        for val in row:
            site.append(str(val))
        sites.append(site)

    for site in sites:
        break
        cur = conn.execute("SELECT URL FROM VIDEOS WHERE ID = (SELECT MAX(ID) FROM VIDEOS WHERE SITEID = :siteid);",
                           {"siteid": site[0]})
        for val in cur:
            site[1] = str(val[0])
    conn.close()
    return sites
