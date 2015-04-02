# -*- coding: UTF-8 -*- 

from bs4 import BeautifulSoup
import socket
import urllib2
import codecs

DOUBANURL = 'http://www.douban.com/group/tomorrow/discussion?start='



#u"获取网页源码"
def get_page_source(url, timeout = 500, coding = None):
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


#u"获取源码中得帖子名称和点击量"
def get_post_num(url):
    try:
        data = get_page_source(url)
        if data[0] == "200":
            soup = BeautifulSoup(data[1])
            trlist = soup.find_all('tr')
            for item in trlist:
                if len(item.contents) == 9:
                    post_title = item.contents[1].get_text()
                    post_link = item.contents[1].find('a').get('href')
                    post_view = item.contents[5].get_text()
                    douban_post = codecs.open('douban.txt', 'a', 'utf-8')
           
                    complite_word = post_link + '\t' + post_title + '\t' + post_view
                    complite_word = complite_word.replace('\n','')
                    douban_post.write(unicode(complite_word))
                    douban_post.write('\n')
                                   
    except Exception,e:
        print str(e)
          

if __name__ == "__main__":

    for page_num in range(1246):
        print '********page %s********' %(page_num+1)
        douban_link = DOUBANURL + str(25*page_num)
        get_post_num(douban_link)



    # get_post_num(DOUBANURL)

