# -*- coding: UTF-8 -*- 

from bs4 import BeautifulSoup
import socket
import urllib2
import codecs

HEXUNURL = 'http://news.search.hexun.com/news?key=%D6%D0%D0%A1%C6%F3%D2%B5&s=1&page=1&t=30&f=1'
HEXUNURL2 = 'http://news.search.hexun.com/news?key=%D6%D0%D0%A1%C6%F3%D2%B5&s=1&page=2&t=30&f=1'

NWURL = 'http://10.23.9.129/html/zwxx/sxzwxx/'  
NWURL2 = 'http://10.23.9.129/html/zwxx/sxzwxx/index-2.html'   

NWORIGINURL = 'http://10.23.9.129'


#u"获取网页源码"
def get_page_source(url, timeout = 100, coding = None):
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


#u"获取源码中得超链接"
def get_hyper_links(url, key_word):
    try:
        links = []
        data = get_page_source(url)
        if data[0] == "200":
            soup = BeautifulSoup(data[1])
            a = soup.find_all('a')
            for i in a:
                target_link = i.get('href')
                if target_link is not None :
                    if target_link.find(key_word) > 0:
                        links.append(target_link) 
                                   
        return links
    except Exception,e:
        print str(e)
        return None  


#u"写入文本文件网择"
def write_into_text_wz(url):
    order_links = get_hyper_links(url, '2015')
    for link in order_links:
        data = get_page_source(link)
        if data[0] == "200":
            soup = BeautifulSoup(data[1])
            try:
                for page_title in soup.find_all("h1"):
                    if page_title.string is not None:
                        pre_title = page_title.string + '.txt'
                        print pre_title
                        html_file = codecs.open(pre_title, 'wb', 'utf-8') 
                        html_file.write(unicode(page_title.string))  
                        html_file.write('\n')
                for page_content in soup.find_all("p"):
                    if page_content.string is not None:
                        page_content_unicode = unicode(page_content.string)
                        html_file.write(page_content_unicode)   
                        html_file.write('\n') 
                html_file.close()  

            except Exception,e:
                print str(e)
                # return None     
                continue



#u"写入文本文件内网"
def write_into_text_nw(url):
    order_links = get_hyper_links(url, 'sxgzjl/2015')
    for link in order_links:
        link = NWORIGINURL + link
        data = get_page_source(link)
        if data[0] == "200":
            soup = BeautifulSoup(data[1])
            try:
                for page_title in soup.find_all("b"):
                    if page_title.string is not None:
                        pre_title = page_title.string + '.txt'
                        pre_title = pre_title.replace("\r\n", "")
                        print pre_title
                        html_file = codecs.open(pre_title, 'wb', 'utf-8') 
                        html_file.write(unicode(page_title.string))  
                        html_file.write('\n')
                for page_content in soup.find_all("p"):
                    page_content_unicode = unicode(page_content.get_text())
                    html_file.write(page_content_unicode)   
                    html_file.write('\n') 
                html_file.close()  

            except Exception,e:
                print str(e)
                # return None     
                continue      



if __name__ == "__main__":
    write_into_text_wz(HEXUNURL)
    # write_into_text_wz(HEXUNURL2)

    write_into_text_nw(NWURL)
    # write_into_text_nw(NWURL2)

