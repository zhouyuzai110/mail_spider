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


mailre = re.compile(r"([0-9a-zA-Z_.-]+@[0-9a-zA-Z.-]+)")
# mailre = re.compile(r"^[a-zA-Z][a-zA-Z0-9_.-]*@[0-9a-zA-Z]+(.[a-zA-Z]+)+$")
mail_out = open("mail_out.txt","a")
mail_list = []
origin_url = "http://www.gjh-enterprise.com/"


class MyCrawler:
    def __init__(self,seeds):
        self.order = 1
        #u"使用种子初始化url队列"
        self.linkQuence=linkQuence()
        if isinstance(seeds,str):
            self.linkQuence.addUnvisitedUrl(seeds)
        if isinstance(seeds,list):
            for i in seeds:
                self.linkQuence.addUnvisitedUrl(i)
        print "Add the seeds url \"%s\" to the unvisited url list"%str(self.linkQuence.unVisited)
    

    #u"抓取过程主函数"
    def crawling(self,seeds,crawl_count):

        #u"循环条件：待抓取的链接不空且专区的网页不多于crawl_count"
        while self.linkQuence.unVisitedUrlsEnmpy() is False and self.linkQuence.getVisitedUrlCount() <= crawl_count:
            #u"队头url出队列"
            visitUrl=self.linkQuence.unVisitedUrlDeQuence()
            print "Pop out one url \"%s\" from unvisited url list"%visitUrl
            if visitUrl is None or visitUrl=="":
                continue
            #u"获取超链接"
            links=self.getHyperLinks(visitUrl)
            print "Get %d new links"%len(links)
            #u"将url放入已访问的url中"
            self.linkQuence.addVisitedUrl(visitUrl)
            self.getEmailAddress(visitUrl)
            print "Visited url count: "+str(self.linkQuence.getVisitedUrlCount())
            #u"未访问的url入列"
            for link in links:
                self.linkQuence.addUnvisitedUrl(link)
                # self.getEmailAddress(link)
            print "%d unvisited links:"%len(self.linkQuence.getUnvisitedUrl())

            
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
                

    

    #u"获取网页源码"
    def getPageSource(self,url,timeout=1,coding=None):
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
    

    def getEmailAddress(self,url,timeout=1):
        try:
            socket.setdefaulttimeout(timeout)
            mail_req = urllib2.Request(url)
            mail_req.add_header('User-agent', 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)')
            mail_response = urllib2.urlopen(mail_req)
            mail_html = mail_response.read()
            mail_target = mailre.findall(mail_html)
            for ix in mail_target:
                if ix not in mail_list:
                    # print ix
                    mail_list.append(ix)
                    # mail_out.write(ix)
                    # mail_out.write("\n")
                    mydb = MySQLdb.connect(
                      host   = "sqld.duapp.com",
                      port   = 4050,
                      user   = api_key,
                      passwd = secret_key,
                      db = dbname)
                    cursor = mydb.cursor()
                    valuesToInsert = [self.order,ix]
                    try:
                        n=cursor.execute("INSERT INTO `gjh-enterprise`(`order`, `mailAddress`) VALUES (%s,%s)",valuesToInsert)
                        # print n
                    except MySQLdb.Error,e:
                        print "Mysql Error %d: %s" % (e.args[0], e.args[1]) 
                    self.order += 1    
                    mydb.commit()    
                    cursor.close()       
                    mydb.close()
                
        except Exception as e:
            print str(e)
            return [str(e),None]    


class linkQuence:
    def __init__(self):
        #u"已访问的url集合"
        self.visted=[]
        #u"待访问的url集合"
        self.unVisited=[]
    #u"获取访问过的url队列"
    def getVisitedUrl(self):
        return self.visted
    #u"获取未访问的url队列"
    def getUnvisitedUrl(self):
        return self.unVisited
    #u"添加到访问过得url队列中"
    def addVisitedUrl(self,url):
        self.visted.append(url)
    #u"移除访问过得url"
    def removeVisitedUrl(self,url):
        self.visted.remove(url)
    #u"未访问过得url出队列"
    def unVisitedUrlDeQuence(self):
        try:
            return self.unVisited.pop()
        except:
            return None
    #u"保证每个url只被访问一次"
    def addUnvisitedUrl(self,url):
        if url!="" and url not in self.visted and url not in self.unVisited:
            self.unVisited.insert(0,url)
    #u"获得已访问的url数目"
    def getVisitedUrlCount(self):
        return len(self.visted)
    #u"获得未访问的url数目"
    def getUnvistedUrlCount(self):
        return len(self.unVisited)
    #u"判断未访问的url队列是否为空"
    def unVisitedUrlsEnmpy(self):
        return len(self.unVisited)==0
   

def main(seeds,crawl_count):
    craw=MyCrawler(seeds)
    craw.crawling(seeds,crawl_count)
if __name__=="__main__":
    main("http://www.gjh-enterprise.com/",3000000)
    # main(["http://www.baidu.com","http://www.google.com.hk"],50)
