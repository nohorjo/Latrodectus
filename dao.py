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


def getId(url):
    while True:
        try:
            conn = sqlite3.connect(dbname)
            cur = conn.cursor()
            cur.execute("SELECT IFNULL((SELECT ID FROM VIDEOS WHERE URL=:url),IFNULL(MAX(ID), 0) + 1)  FROM VIDEOS;",
                        {"url": url})
            result = -1
            for row in cur:
                result = int(row[0])
            conn.close()
            return result
        except sqlite3.OperationalError:
            pass
        break


def addUrl(siteid, url, checkedstatus):
    while True:
        try:
            vId = getId(url)
            conn = sqlite3.connect(dbname)
            conn.execute(
                "REPLACE INTO VIDEOS (ID, SITEID, URL, CHECKED) VALUES (:id,:siteid,:url,:checked);",
                {"id": vId, "siteid": siteid, "url": url, "checked": checkedstatus})
            conn.commit()
            conn.close()
        except sqlite3.OperationalError:
            pass
        break


def vidStatus(url):
    while True:
        try:
            conn = sqlite3.connect(dbname)
            cur = conn.cursor()
            cur.execute("SELECT CHECKED FROM VIDEOS WHERE URL=:url;", {"url": url})
            result = -1
            for row in cur:
                result = int(row[0])
            conn.close()
            return result
        except sqlite3.OperationalError:
            pass


def isBlackListed(tags):
    while True:
        try:
            result = False
            tags = [str(x).strip() for x in tags]
            if len(tags) != 0:
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
                for row in cur:
                    if int(row[0]) > 0:
                        result = True
                conn.close()
            return result
        except sqlite3.OperationalError:
            pass


def getSites():
    while True:
        try:
            sites = []
            conn = sqlite3.connect(dbname)
            cur = conn.execute("SELECT ID, SITE, LINK, NAME, DURATION, DATE, TAGS FROM SITES;")
            for row in cur:
                site = []
                for val in row:
                    site.append(str(val))
                sites.append(site)
            conn.close()
            return sites
        except sqlite3.OperationalError:
            pass


def getResumeUrl(urlid, url):
    while True:
        try:
            conn = sqlite3.connect(dbname)
            cur = conn.execute(
                '''SELECT URL FROM VIDEOS WHERE ID = (SELECT ID FROM VIDEOS WHERE SITEID = :siteid AND CHECKED < 2
                    ORDER BY RANDOM() LIMIT 1);''',
                {"siteid": urlid})
            for val in cur:
                url = str(val[0])
            conn.close()
            return url
        except sqlite3.OperationalError:
            pass


def clean():
    try:
        conn = sqlite3.connect(dbname)
        conn.execute("DELETE FROM VIDEOS WHERE URL IN (SELECT SITE FROM SITES);")
        conn.close()
    except sqlite3.OperationalError:
        pass


def getToDownload():
    while True:
        try:
            conn = sqlite3.connect(dbname)
            cur = conn.execute(
                '''SELECT ID, URL, SITEID, CHECKED FROM VIDEOS WHERE CHECKED IN (1, 3) ORDER BY RANDOM() LIMIT 1;''')
            for val in cur:
                vId = str(val[0])
                url = str(val[1])
                urlid = str(val[2])
                checked = int(val[3])
            conn.close()
            return {"id": vId, "url": url, "urlid": urlid, "status": checked}
        except sqlite3.OperationalError:
            pass


if __name__ == "__main__":
    init()
