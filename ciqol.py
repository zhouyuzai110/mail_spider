# -*- coding: UTF-8 -*- 

from bs4 import BeautifulSoup
import socket
import urllib2
import re
import threading
import Queue
import MySQLdb

dbname = "bxWXpbFfNCAATdSdSaQh"
api_key = "mpbluSASap9EPbqnFQ39WPDK"
secret_key = "GHqF6rGnw5X80XTOUCRnPTMbUkaTlIa8"
table_name = "www.ciqol.com"

queue = Queue.Queue(maxsize = 9)
MySQLqueue = Queue.Queue(maxsize = 3)
mailre = re.compile(r"([0-9a-zA-Z_.-]+@[0-9a-zA-Z_.-]+)")


class MyCrawler(threading.Thread):
    def __init__(self,queue,MySQLqueue):
        threading.Thread.__init__(self)
        self.queue = queue
        self.MySQLqueue = MySQLqueue
 
    #u"抓取过程主函数"
    def run(self): 
        while True:
            try:
                visitUrl = self.queue.get()
                print visitUrl
                #u"提取邮箱地址 返回一个列表 加入待写入数据库队列"
                maillist = self.getEmailAddress(visitUrl)
                if maillist is not None:
                    for mailhit in maillist:
                        if mailhit is not None:
                            tmail = (mailhit,visitUrl)
                            MySQLqueue.put(tmail)
                self.queue.task_done()

            except Exception,e:
                print str(e)  
                # self.dbconn()
                # sql = "INSERT INTO `www.ciqol.com-error`(`httpAddress`) values(%s)"                
                # self.cursor.execute(sql,visitUrl)
                # self.conn.commit()  
                # self.dbClose()   

    def getEmailAddress(self,url,timeout = 100):
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
                
#u"获取源码中得超链接"
def getHyperLinks(url):
    try:
        links = []
        data = getPageSource(url)
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
                    target_link = url + i["href"]
                    links.append(target_link) 
        return links
    except Exception,e:
        print str(e)
        return None  


#u"获取网页源码"
def getPageSource(url, timeout = 100, coding = None):
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
    

   


class MySQLQuence(threading.Thread):
    def __init__(self,MySQLqueue): 
        threading.Thread.__init__(self)
        self.MySQLqueue = MySQLqueue      
        self.host = 'sqld.duapp.com'
        self.user = api_key
        self.passwd = secret_key
        self.db = dbname
        self.port = 4050
        
     #u"插入邮箱地址"    
    def run(self):
        while True:
            try:
                inmail = self.MySQLqueue.get()
                print  "========== Conn To MySQL Database, Please Waiting =========="
                self.dbconn()
                # sql = "INSERT INTO `www.ciqol.com`(`mailAddress`,`httpAddress`) SELECT %s,%s FROM dual WHERE not exists (select * from `gjh-enterprise` where mailAddress = %s)"
                sql = "INSERT INTO `www.ciqol.com`(`mailAddress`,`httpAddress`) values(%s,%s)"                
                self.cursor.execute(sql,inmail)
                self.conn.commit()  
                self.dbClose() 
                print "========== All Things Are Clean, Close MySQL Database =========="
                self.MySQLqueue.task_done()     
            except MySQLdb.Error,e:
                print "INSERT INTO `www.ciqol.com` Mysql Error %d: %s" % (e.args[0], e.args[1]) 

           
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

 
#     #u"保证每个url只被访问一次,插入的链接唯一"    
#     def addUnvisitedUrl(self,urllist):
#         try:       
#             sql = "INSERT INTO `linkQuence`(`linkAddress`,`visited`) SELECT %s,%s FROM dual WHERE not exists (select * from `linkQuence` where linkAddress = %s )"
#             self.cursor.executemany(sql,urllist)
#             self.conn.commit()   
#         except MySQLdb.Error,e:
#             print "INSERT INTO `linkQuence` Mysql Error %d: %s" % (e.args[0], e.args[1])      


#     #u"获得已访问的url数目"
#     def getVisitedUrlCount(self):
#         sql = "SELECT count(*) from `linkQuence` where visited = 1" 
#         self.cursor.execute(sql)
#         result = self.cursor.fetchone()
#         return int(result[0]) 

          
#     #u"获得未访问的url数目"
#     def getUnVisitedUrlCount(self):
#         sql = "SELECT count(*) from `linkQuence` where visited = 0"
#         self.cursor.execute(sql)
#         result = self.cursor.fetchone()
#         return int(result[0]) 


#     #u"访问过得url visited 变成1"
#     def addVisitedUrl(self,urllist):
#         try:
#             sql = "UPDATE `linkQuence` SET `visited` = %s WHERE linkAddress = %s"
#             self.cursor.executemany(sql,urllist)
#             self.conn.commit()
#         except MySQLdb.Error,e:
#             print "UPDATE `linkQuence` Mysql Error %d: %s" % (e.args[0], e.args[1]) 
                

#     #u"判断未访问的url队列是否为空"
#     def unVisitedUrlsEnmpy(self):
#         sql = "SELECT count(*) from `linkQuence` where visited = 0"
#         self.cursor.execute(sql)
#         result = self.cursor.fetchone()
#         return int(result[0]) == 0 


#     #u"未访问过得url出队列"
#     def unVisitedUrlDeQuence(self):
#         try:
#             unVisitList = []
#             sql = "SELECT linkAddress from `linkQuence` where visited = 0 limit 10"
#             self.cursor.execute(sql)
#             results = self.cursor.fetchall()
#             for link in results:
#                unVisitList.append(str(link[0])) 
#             return unVisitList
#         except:
#             return None   


def main(): 
    for i in range(9):
        t = MyCrawler(queue,MySQLqueue)
        t.setDaemon(True)
        t.start()

    for i in range(3):
        t = MySQLQuence(MySQLqueue)
        t.setDaemon(True)
        t.start()

    for i in range(825475,3660000):
        try:
            visitUrl = "http://information.ciqol.com/buyernew/view/type/buyer/id/%d" %i
            queue.put(visitUrl)
            #u"把链接放入待访问队列"
        except Exception,e:
            print str(e)           
 

    queue.join()
    MySQLqueue.join()

if __name__ == "__main__":
    main()



