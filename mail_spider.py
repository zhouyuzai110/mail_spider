# -*- coding: UTF-8 -*- 
#http://www.verydemo.com/demo_c122_i1094.html


from bs4 import BeautifulSoup
import socket
import urllib2
import re
import MySQLdb

dbname = "bxWXpbFfNCAATdSdSaQh"
api_key = "mpbluSASap9EPbqnFQ39WPDK"
secret_key = "GHqF6rGnw5X80XTOUCRnPTMbUkaTlIa8"
table_name = "gjh-enterprise"


mailre = re.compile(r"([0-9a-zA-Z_.-]+@[0-9a-zA-Z.]+)")
origin_url = "http://www.gjh-enterprise.com/"


class MyCrawler:
    def __init__(self,seeds):
        #u"使用种子初始化url队列"
        self.MySQLQuence1 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)
        if isinstance(seeds,str):
            self.MySQLQuence1.addUnvisitedUrl(seeds)
            print "Add the seeds url \"%s\" to the unvisited url list" %seeds
        if isinstance(seeds,list):
            for i in seeds:
                self.MySQLQuence1.addUnvisitedUrl(i)
        # print "Add the seeds url \"%s\" to the unvisited url list"%str(self.MySQLQuence.unVisited)
    

    #u"抓取过程主函数"
    def crawling(self,seeds,crawl_count):
        #u"循环条件：待抓取的链接不空且抓取的网页不多于crawl_count"
        self.MySQLQuence2 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)
        unVisitedUrlsEnmpy = self.MySQLQuence2.unVisitedUrlsEnmpy()
        print unVisitedUrlsEnmpy
        self.MySQLQuence3 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)
        VisitedUrlCount = self.MySQLQuence3.getVisitedUrlCount()
        print VisitedUrlCount
        while  unVisitedUrlsEnmpy is False and VisitedUrlCount <= crawl_count:
            try:
                #u"队头url出队列"
                self.MySQLQuence4 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)
                visitUrlList = self.MySQLQuence4.unVisitedUrlDeQuence()
                print "Pop out one url \"%s\" from unvisited url list" %visitUrlList
                for visitUrlx in visitUrlList:
                    visitUrl = str(visitUrlx[0])
                    if visitUrl is None or visitUrl=="":
                        continue
                    #u"获取超链接"
                    links=self.getHyperLinks(visitUrl)
                    print "Get %d new links"%len(links)
                    #u"将url放入已访问的url中"
                    self.MySQLQuence5 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)
                    self.MySQLQuence5.addVisitedUrl(visitUrl)
                    self.getEmailAddress(visitUrl)
                    self.MySQLQuence6 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)
                    print "Visited url count: "+str(self.MySQLQuence6.getVisitedUrlCount())
                    #u"未访问的url入列"
                    for link in links:
                        self.MySQLQuence7 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)
                        self.MySQLQuence7.addUnvisitedUrl(link)
                    self.MySQLQuence8 = MySQLQuence('sqld.duapp.com', api_key, secret_key, dbname, 4050)    
                    print "%s unvisited links:" %self.MySQLQuence8.getUnVisitedUrlCount()
            except Exception,e:
                print str(e)    

            
    #u"获取源码中得超链接"
    def getHyperLinks(self,url):
        try:
            links=[]
            data=self.getPageSource(url)
            if data[0]=="200":
                soup=BeautifulSoup(data[1])
                a=soup.findAll("a",{"href":re.compile(".*")})
                for i in a:
                    if "index" in i["href"] and "index" not in url:
                        target_link = url + i["href"]
                        links.append(target_link) 
                    elif "index" in i["href"] and "index"  in url:
                        newUrl = re.sub("index-\d+.html","",url) + i["href"]
                        links.append(newUrl)
                    else:
                        target_link = origin_url + i["href"]
                        links.append(target_link) 
            return links
        except Exception,e:
            print str(e)
            return None  
    

    #u"获取网页源码"
    def getPageSource(self,url,timeout=10,coding=None):
        try:
            socket.setdefaulttimeout(timeout)
            req = urllib2.Request(url)
            req.add_header('User-agent', 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)')
            response = urllib2.urlopen(req)
            if coding is None:
                coding= response.headers.getparam("charset")
            if coding is None:
                page=response.read()
            else:
                page=response.read()
                page=page.decode(coding).encode('utf-8')

            return ["200",page]
        except Exception,e:
            print str(e)
            return [str(e),None]
    

    def getEmailAddress(self,url,timeout=10):
        try:
            socket.setdefaulttimeout(timeout)
            mail_req = urllib2.Request(url)
            mail_req.add_header('User-agent', 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)')
            mail_response = urllib2.urlopen(mail_req)
            mail_html = mail_response.read()
            mail_target = mailre.findall(mail_html)
            for ix in mail_target:
                mydb = MySQLdb.connect(
                  host   = "sqld.duapp.com",
                  port   = 4050,
                  user   = api_key,
                  passwd = secret_key,
                  db = dbname)
                cursor = mydb.cursor()
                valuesToInsert = [ix,ix]
                try:
                    cursor.execute("INSERT INTO `gjh-enterprise`(`mailAddress`) SELECT %s FROM dual WHERE not exists (select * from `gjh-enterprise` where mailAddress = %s)",valuesToInsert)
                except MySQLdb.Error,e:
                    print "Mysql Error %d: %s" % (e.args[0], e.args[1]) 
                mydb.commit()    
                cursor.close()       
                mydb.close()
                
        except Exception as e:
            print str(e)
            return None   
   


class MySQLQuence:
    def __init__(self, host,  user, passwd, db, port):
    # def __init__(self, host = 'sqld.duapp.com', port = 4050, user = api_key, passwd = secret_key, db = dbname):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        try:
            self.conn = MySQLdb.connect(self.host, self.user, self.passwd, self.db, self.port)
        except MySQLdb.Error,e:
            errormsg = 'Cannot connect to server\nERROR (%s): %s' %(e.args[0],e.args[1])
            print errormsg
        self.cursor = self.conn.cursor()

 
    #u"保证每个url只被访问一次,插入的链接唯一"    
    def addUnvisitedUrl(self,url):
        sql = "INSERT INTO `linkQuence`(`linkAddress`, `visited`) SELECT %s,%s FROM dual WHERE not exists (select * from `linkQuence` where linkAddress = %s )"
        self.cursor.execute(sql,[url,0,url])
        self.conn.commit()    
        self.cursor.close()       
        self.conn.close()


    #u"获得已访问的url数目"
    def getVisitedUrlCount(self):
        sql = "SELECT * from `linkQuence` where visited = 1" 
        count = self.cursor.execute(sql)
        return count 
        self.cursor.close()       
        self.conn.close()   
          
    #u"获得未访问的url数目"
    def getUnVisitedUrlCount(self):
        sql = "SELECT * from `linkQuence` where visited = 0"
        count = self.cursor.execute(sql)
        return count  
        self.cursor.close()       
        self.conn.close()  

    #u"访问过得url visited 变成1"
    def addVisitedUrl(self,url):
        sql = "UPDATE `linkQuence` SET `visited` = %s WHERE linkAddress = %s"
        self.cursor.execute(sql,[1,url])
        self.conn.commit()
        self.cursor.close()       
        self.conn.close()

    #u"判断未访问的url队列是否为空"
    def unVisitedUrlsEnmpy(self):
        sql = "SELECT * from `linkQuence` where visited = 0"
        count = self.cursor.execute(sql)
        return count == 0 
        self.cursor.close()       
        self.conn.close()   


    #u"未访问过得url出队列"
    def unVisitedUrlDeQuence(self):
        try:
            linkslist = []
            sql = "SELECT linkAddress from `linkQuence` where visited = 0 limit 30"
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for i in results:
                linkslist.append(i)
            return linkslist
            self.cursor.close()       
            self.conn.close()
        except:
            return None   
            self.cursor.close()       
            self.conn.close() 


def main(seeds,crawl_count):
    craw = MyCrawler(seeds)
    craw.crawling(seeds,crawl_count)
if __name__=="__main__":
    main("http://www.gjh-enterprise.com/",3000000)
    # main(["http://www.baidu.com","http://www.google.com.hk"],50)


