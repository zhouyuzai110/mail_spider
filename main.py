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


mailre = re.compile(r"([0-9a-zA-Z_.-]+@[0-9a-zA-Z_.-]+)")
origin_url = "http://www.gjh-enterprise.com/"


class MyCrawler:
    def __init__(self,seeds):
        #u"使用种子初始化url队列"
        self.MySQLQuence = MySQLQuence()
        self.MySQLQuence.dbconn()
        self.MySQLQuence.addUnvisitedUrl([(seeds,0,seeds)])
        self.MySQLQuence.dbClose()
        print "Add the seeds url \"%s\" to the unvisited url list" %seeds

    

    #u"抓取过程主函数"
    def crawling(self,seeds,crawl_count):
        self.MySQLQuence.dbconn()
        #u"判断未访问列表是否为空"
        unVisitedUrlsEnmpy = self.MySQLQuence.unVisitedUrlsEnmpy()
        #u"计算已访问链接数"
        VisitedUrlCount = self.MySQLQuence.getVisitedUrlCount()
        self.MySQLQuence.dbClose()
        #u"循环条件：待抓取的链接不空且抓取的网页不多于crawl_count"
        while  unVisitedUrlsEnmpy is False and VisitedUrlCount <= crawl_count:
            try:
                #u"待访问url出队列"
                self.MySQLQuence.dbconn()
                visitUrlList = self.MySQLQuence.unVisitedUrlDeQuence()
                #self.MySQLQuence.dbClose()
                print "Pop out urls \"%s\" from unvisited url list" %visitUrlList
                
                urlLinkList = []
                updateLinkList = []
                mailAddressList = []
                mailAddressInsertList = []

                for visitUrl in visitUrlList:
                    if visitUrl is None or visitUrl == "":
                        continue

                    #u"获取超链接"
                    links = self.getHyperLinks(visitUrl)
                    print visitUrl
                    print "Get %d new links" %len(links)

                    #u"提取邮箱地址 返回一个列表"
                    maillist = self.getEmailAddress(visitUrl)
                    if maillist is not None:
                        mailAddressList.extend(maillist)

                    #u"制作未访问的url加入待插入数据库队列"
                    for link in links:
                        if link is not None:
                            tlink = (link,0,link)
                            urlLinkList.append(tlink)

                #u"制作待插入邮箱地址列表"
                for maillink in mailAddressList:
                    if maillink is not None:
                        tmail = (maillink,maillink)
                        mailAddressInsertList.append(tmail)

                #u"将url放入已访问的url中"
                for visitUrl in visitUrlList:
                    updateLink = (1,visitUrl)
                    updateLinkList.append(updateLink)
                    
                urlLinkListNoCopy = set(urlLinkList)
                
                print "Total Ready To Insert MySQL Database Links Are: %d" %len(urlLinkListNoCopy)
                print "----------Conn To MySQL Database, Please Waiting----------"
                #self.MySQLQuence.dbconn()    
                #u"批量插入邮箱地址列表"
                self.MySQLQuence.insertMailList(mailAddressInsertList)
                #u"批量插入待访问地址列表"
                self.MySQLQuence.addUnvisitedUrl(urlLinkListNoCopy)
	            #u"批量更改访问过地址列表状态"
                self.MySQLQuence.addVisitedUrl(updateLinkList)

                # print "Visited Url Count: "+str(self.MySQLQuence.getVisitedUrlCount())
                # print "%s Unvisited Links " %self.MySQLQuence.getUnVisitedUrlCount()
                self.MySQLQuence.dbClose()

                print "----------All Things Are Clean, Close MySQL Database----------"
            except Exception,e:
                print str(e)    

            
    #u"获取源码中得超链接"
    def getHyperLinks(self,url):
        try:
            links = []
            data = self.getPageSource(url)
            if data[0]=="200":
                soup=BeautifulSoup(data[1])
                a=soup.findAll("a",{"href":re.compile(".*")})
                for i in a:
                    if "index" in i["href"] and "index" not in url:
                        target_link = url + "/" + i["href"]
                        links.append(target_link) 
                    elif "index" in i["href"] and "index"  in url:
                        newUrl = re.sub("index-\d+.html","",url) + "/" + i["href"]
                        links.append(newUrl)
                    else:
                        target_link = origin_url + i["href"]
                        links.append(target_link) 
            return links
        except Exception,e:
            print str(e)
            return None  
    

    #u"获取网页源码"
    def getPageSource(self, url, timeout = 10, coding = None):
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
            return mail_target        
        except Exception as e:
            print str(e)
            return None   
   


class MySQLQuence:
    def __init__(self):       
        self.host = 'sqld.duapp.com'
        self.user = api_key
        self.passwd = secret_key
        self.db = dbname
        self.port = 4050
	    
        
    
    def dbconn(self):
        try:
            self.conn = MySQLdb.connect(self.host, self.user, self.passwd, self.db, self.port)
        except MySQLdb.Error,e:
            errormsg = 'Cannot connect to server\nERROR (%s): %s' %(e.args[0],e.args[1])
            print errormsg
        self.cursor = self.conn.cursor()


    def dbClose(self):
        self.cursor.close()       
        self.conn.close()

 
    #u"保证每个url只被访问一次,插入的链接唯一"    
    def addUnvisitedUrl(self,urllist):
        try:       
            sql = "INSERT INTO `linkQuence`(`linkAddress`,`visited`) SELECT %s,%s FROM dual WHERE not exists (select * from `linkQuence` where linkAddress = %s )"
            self.cursor.executemany(sql,urllist)
            self.conn.commit()   
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1])      


    #u"批量插入邮箱地址"    
    def insertMailList(self,urllist):
        try:
            sql = "INSERT INTO `gjh-enterprise`(`mailAddress`) SELECT %s FROM dual WHERE not exists (select * from `gjh-enterprise` where mailAddress = %s)"
            self.cursor.executemany(sql,urllist)
            self.conn.commit()    
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1]) 


    #u"获得已访问的url数目"
    def getVisitedUrlCount(self):
        sql = "SELECT count(*) from `linkQuence` where visited = 1" 
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return int(result[0]) 

          
    #u"获得未访问的url数目"
    def getUnVisitedUrlCount(self):
        sql = "SELECT count(*) from `linkQuence` where visited = 0"
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return int(result[0]) 


    #u"访问过得url visited 变成1"
    def addVisitedUrl(self,urllist):
        try:
            sql = "UPDATE `linkQuence` SET `visited` = %s WHERE linkAddress = %s"
            self.cursor.executemany(sql,urllist)
            self.conn.commit()
        except MySQLdb.Error,e:
            print "Mysql Error %d: %s" % (e.args[0], e.args[1]) 
                

    #u"判断未访问的url队列是否为空"
    def unVisitedUrlsEnmpy(self):
        sql = "SELECT count(*) from `linkQuence` where visited = 0"
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return int(result[0]) == 0 


    #u"未访问过得url出队列"
    def unVisitedUrlDeQuence(self):
        try:
            unVisitList = []
            sql = "SELECT linkAddress from `linkQuence` where visited = 0 limit 30"
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for link in results:
               unVisitList.append(str(link[0])) 
            return unVisitList
        except:
            return None   


def main(seeds,crawl_count):
    craw = MyCrawler(seeds)
    craw.crawling(seeds,crawl_count)
if __name__ == "__main__":
    main("http://www.gjh-enterprise.com/",3000000)


