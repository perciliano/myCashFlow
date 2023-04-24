#' title......: HTTP, HTTPS, Scraping
#' description: Leitura de websites para scraping por objetos
#' file.......: scrap.py
#' version....: 0.1.1
#' author.....: Carlos Perciliano Gaudencio
#' date.......: 2021-01-01
#' update.....: 2022-07-30
#' library....: requests, re, BeautifulSoup, xmltodict, etree.ElementTree  https://www.pluralsight.com/guides/web-scraping-with-request-python ; https://docs.python.org/3/library/html.parser.html
#' sample.....: from scrap.scrap import ScrapingO   ; sp=ScrapingO() /  ; sp.testRe()
#    sp=ScrapingO(attribs={'q':'dollar+rate+today'})  ; sp.htmlRead() ; sp.soup.body.find_all(attrs={"class": "kCrYT"})[1].text ; sp.htmlRead(); sp.soup.body.find_all(attrs={"class": "kCrYT"})[1].text.replace('Exoneração de responsabilidade','')
#    sj=ScrapingO(url='https://jsonplaceholder.typicode.com/posts',attribs={'title':'Python Requests','body':'Requests are awesome','userId':1},request='post') ; sj.testRe()
#    er=ScrapingO(url='http://localhost')
#    bc=ScrapingO(url='https://www3.bcb.gov.br/bc_moeda/rest/cotacao/fechamento/ultima/1/220/2011-01-01') ; bc.xmlRead() ;  bc.htmlRead(features='xml') ; print(bc.soup.prettify()) ;https://www.bcb.gov.br/conversao
class ScrapingO():
    def __init__(self,url='https://www.google.com/search',attribs={},request='get',stream=False,timeout=3,redirect=False,level=1,headers=None):
        if (request=='file'):
            self.__htmlFile(url,level)
            return
        import requests
        try:
            self.ok=False
            if (request=='put'):
                self.response=requests.put(url,attribs,stream,allow_redirects=redirect,timeout=timeout)
            elif (request=='post'):
                self.response=requests.post(url,attribs,stream,allow_redirects=redirect,timeout=timeout)
            elif (request=='delete'):
                self.response=requests.delete(url,attribs,allow_redirects=redirect,timeout=timeout)
            else:
                self.response=requests.get(url,attribs,stream=stream,allow_redirects=redirect,timeout=timeout,headers=headers)
            self.ok=self.response.ok
            self.level=level
            if (1<=level):
                self.status=self.response.status_code
                self.url=self.response.url
                self.request=self.response.request
                self.content=self.response.content
            #1
            if (2<=level):
                self.encoding=self.response.encoding
                self.raise_for_status=self.response.raise_for_status
            #2
            if (3<=level):
                self.text=self.response.text
                self.headers=self.response.headers
                self.cookies=self.response.cookies
                self.json=self.response.json
            #3
            self.elapsed=self.response.elapsed
        except requests.exceptions.Timeout as timeOutErr: 
            self.err=f'Timeout Error:['+str(timeOutErr)+f'].'
        except requests.exceptions.HTTPError as httpErr: 
            self.err=f'Http Error:['+str(httpErr)+f'].'
        except requests.exceptions.ConnectionError as connErr: 
            self.err=f'Error Connecting:['+str(connErr)+f'].'
        except requests.exceptions.RequestException as reqErr: 
            self.err=f'Something Else:['+str(reqErr)+f'].'
        except Exception as err:
            self.err=f'error occurred:['+str(err)+f'].'
        else:
            self.err=self.response.status_code
        finally:
            if (self.ok==False): print('End:'+str(self.err))
    def __htmlFile(self,url,level):
        from bs4 import BeautifulSoup
        try:
            self.ok=False
            self.soup=BeautifulSoup(open(url,'r').read(),'html.parser')
            self.content=self.soup.prettify()
            self.ok=True
            self.level=level
            self.status=200
            self.url=url
            self.text=self.soup.text
        except Exception as err:
            self.err=f'file error occurred:['+str(err)+f'].'
        else:
            self.err=200
        finally:
            if (self.ok==False): print('End:'+str(self.err))
    def htmlRead(self,features='html.parser'): #=html.parser, xml, lxml
        from bs4 import BeautifulSoup
        self.soup=BeautifulSoup(self.content,features)
    def xmlRead(self,isTree=False):
        if(isTree):
          from xml.etree.ElementTree import fromstring
          self.tree = fromstring(self.content)
        else:
          import xmltodict
          self.dict = xmltodict.parse(self.content)
    def testRe(self):
        import re
        test=re.compile(r'<[^>]+>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});').sub('',self.response.text)
        test=test[test.find('span{text-align:center}')+23:]
        test=test[:test.find('(function(){var hl=\'pt-BR\';')]
        print(self.status,'\n',self.url,'\n',test)
