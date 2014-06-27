# -*- coding: UTF-8 -*- 
#http://www.verydemo.com/demo_c122_i1094.html

from bs4 import BeautifulSoup
import socket
import urllib2
import re
import MySQLdb
import threading
import Queue

dbname = "bxWXpbFfNCAATdSdSaQh"
api_key = "mpbluSASap9EPbqnFQ39WPDK"
secret_key = "GHqF6rGnw5X80XTOUCRnPTMbUkaTlIa8"
table_name = "gjh-enterprise"

queue = Queue.Queue(maxsize = 8)
mailre = re.compile(r"([0-9a-zA-Z_.-]+@[0-9a-zA-Z_.-]+)")
origin_url = "http://www.gjh-enterprise.com/"


class MyCrawler(threading.Thread):
    def __init__(self,queue):
        #u"使用种子初始化url队列"
        threading.Thread.__init__(self)
        self.queue = queue
        self.MySQLQuence = MySQLQuence()
        self.seeds = origin_url
        self.MySQLQuence.dbconn()
        self.MySQLQuence.addUnvisitedUrl([(self.seeds,0,self.seeds)])
        self.MySQLQuence.dbClose()
        print "Add the seeds url \"%s\" to the unvisited url list" %self.seeds

    

    #u"抓取过程主函数"
    def run(self): 
        while True:
            try:
                visitUrlList = self.queue.get()
                urlLinkList = []
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
                    
                urlLinkListNoCopy = set(urlLinkList)
                
                print "Total Ready To Insert MySQL Database Links Are: %d" %len(urlLinkListNoCopy)
                print "----------Conn To MySQL Database, Please Waiting----------"
                self.MySQLQuence.dbconn()    
                #u"批量插入邮箱地址列表"
                self.MySQLQuence.insertMailList(mailAddressInsertList)
                #u"批量插入待访问地址列表"
                self.MySQLQuence.addUnvisitedUrl(urlLinkListNoCopy)
                

                # print "Visited Url Count: "+str(self.MySQLQuence.getVisitedUrlCount())
                # print "%s Unvisited Links " %self.MySQLQuence.getUnVisitedUrlCount()
                self.MySQLQuence.dbClose()

                print "----------All Things Are Clean, Close MySQL Database----------"
                self.queue.task_done()

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
            sql = "SELECT linkAddress from `linkQuence` where visited = 0 limit 10"
            self.cursor.execute(sql)
            results = self.cursor.fetchall()
            for link in results:
               unVisitList.append(str(link[0])) 
            return unVisitList
        except:
            return None   


def main(): 
    for i in range(5):
        t = MyCrawler(queue)
        t.setDaemon(True)
        t.start()

    while True :
        MySQLQuence1 = MySQLQuence()
        MySQLQuence1.dbconn()
        visitUrlList = MySQLQuence1.unVisitedUrlDeQuence()
        print "Pop out urls \"%s\" from unvisited url list" %visitUrlList
        if visitUrlList is not None:
            queue.put(visitUrlList)
        #u"将url放入已访问的url中"
        updateLinkList = []
        for visitUrl in visitUrlList:
            if visitUrl is not None:
                updateLink = (1,visitUrl)
                updateLinkList.append(updateLink)
        #u"批量更改访问过地址列表状态"
        MySQLQuence1.addVisitedUrl(updateLinkList)
        MySQLQuence1.dbClose()

    queue.join()
    

if __name__ == "__main__":
    main()



